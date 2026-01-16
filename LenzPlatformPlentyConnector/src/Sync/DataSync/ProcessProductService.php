<?php

declare(strict_types=1);

namespace LenzPlatformPlentyConnector\Sync\DataSync;

use LenzPlatformPlentyConnector\Core\Content\ItemUpdate\ItemUpdateCollection;
use LenzPlatformPlentyConnector\Core\Content\ItemUpdate\ItemUpdateEntity;
use LenzPlatformPlentyConnector\Core\Content\MappingField\MappingFieldDefinition;
use LenzPlatformPlentyConnector\Core\Content\MediaLink\MediaLinkEntity;
use LenzPlatformPlentyConnector\Core\Content\TaxLink\TaxLinkEntity;
use LenzPlatformPlentyConnector\Feature\Api\RequestBuilder\GetVariationsBaseRequestBuilder;
use LenzPlatformPlentyConnector\Feature\DataService\Event\CriteriaEvent;
use LenzPlatformPlentyConnector\Feature\DataService\Mapping\DataStore\DataStore;
use LenzPlatformPlentyConnector\Plentymarkets\Api\Request\GetVariationsRequest;
use LenzPlatformPlentyConnector\Plentymarkets\Api\Request\RequestInterface;
use LenzPlatformPlentyConnector\Plentymarkets\Property;
use LenzPlatformPlentyConnector\Service\CommonServiceRegistry;
use LenzPlatformPlentyConnector\Service\ConfigService;
use LenzPlatformPlentyConnector\Service\ProductData\Helper\ImageHelper;
use LenzPlatformPlentyConnector\Sync\DataSync\Mapping\MappingField;
use LenzPlatformPlentyConnector\Sync\DataSync\Mapping\SkipMappingException;
use LenzPlatformPlentyConnector\Sync\ServiceExecutionResult;
use Shopware\Core\Content\Product\Aggregate\ProductVisibility\ProductVisibilityDefinition;
use Shopware\Core\Content\Product\Aggregate\ProductVisibility\ProductVisibilityEntity;
use Shopware\Core\Content\Product\ProductCollection;
use Shopware\Core\Content\Product\ProductEntity;
use Shopware\Core\Defaults;
use Shopware\Core\Framework\DataAbstractionLayer\Entity;
use Shopware\Core\Framework\DataAbstractionLayer\EntityCollection;
use Shopware\Core\Framework\DataAbstractionLayer\EntityRepository;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Criteria;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Filter\EqualsAnyFilter;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Filter\EqualsFilter;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Sorting\FieldSorting;
use Shopware\Core\Framework\Uuid\Uuid;
use Shopware\Core\System\Currency\CurrencyEntity;
use Shopware\Core\System\CustomField\CustomFieldCollection;
use Shopware\Core\System\CustomField\CustomFieldTypes;
use Shopware\Core\System\Language\LanguageEntity;
use Shopware\Core\System\SalesChannel\SalesChannelCollection;
use Shopware\Core\System\SalesChannel\SalesChannelEntity;

class ProcessProductService extends AbstractProcessService
{
    public const DATA_STORE_KEY_EXISTING_SW_PRODUCTS = 'existing-sw-products';
    public const DATA_STORE_KEY_CURRENT_SW_COVER_ID = 'current-sw-cover-id';
    public const DATA_STORE_KEY_CATEGORY_ID_2_SW_ID = 'category-id-2-sw-id';
    public const DATA_STORE_KEY_SALES_CHANNELS = 'sales-channels';
    public const DATA_STORE_KEY_PRODUCT_MAIN_CATEGORY_DELETE = 'product-main-category-delete';
    public const CUSTOM_FIELD_PROPERTY_PREFIX = 'lenz_platform_plenty_connector_product_property';
    public const DATA_STORE_KEY_TAG_ID_2_SW_ID = 'tag-id-2-sw-id';
    public const DATA_STORE_KEY_PRODUCT_TAG_DELETE = 'product-tag-delete';
    protected ?array $propertyGroupIdRestrictions = null;
    protected ?array $tagIdRestrictions = null;
    protected array $productVisibilityDelete = [];
    protected array $productCategoryDelete = [];
    protected array $productOptionDelete = [];
    protected array $productConfiguratorSettingsDelete = [];
    protected array $productConfiguratorSettingsCreate = [];
    protected array $productMediaDelete = [];
    protected array $productPropertyDelete = [];
    protected array $dynamicProperties = [];
    protected int|null $debugVariationId = null;

    public function __construct(CommonServiceRegistry $commonServiceRegistry)
    {
        parent::__construct($commonServiceRegistry);
    }

    public function getInternalName(): string
    {
        return 'process-products';
    }

    public function execute(): ServiceExecutionResult
    {
        $this->propertyGroupIdRestrictions = $this->commonServiceRegistry->getConfigService()->getPropertyGroupIdRestrictions();
        $this->tagIdRestrictions = $this->commonServiceRegistry->getConfigService()->getTagIdRestrictions();
        $this->productConfiguratorSettingsCreate = [];
        $this->productConfiguratorSettingsDelete = [];
        $this->productVisibilityDelete = [];
        $this->productCategoryDelete = [];
        $this->productOptionDelete = [];
        $this->productMediaDelete = [];
        return parent::execute();
    }

    public function setDebugVariationId(int|null $debugVariationId): void
    {
        $this->debugVariationId = $debugVariationId;
    }

    public function getPlentyUpdateRepository(): EntityRepository
    {
        return $this->commonServiceRegistry->getItemUpdateRepository();
    }

    public function getPlentyLinkRepository(): EntityRepository
    {
        return $this->commonServiceRegistry->getItemLinkRepository();
    }

    public function getSwRepository(): EntityRepository
    {
        return $this->commonServiceRegistry->getSwProductRepository();
    }

    protected function getPlentyUpdatesCriteria(): Criteria
    {
        $criteria = parent::getPlentyUpdatesCriteria();
        $originalSorting = $criteria->getSorting();
        $criteria->resetSorting();
        // We need to sort by isMainVariation DESC, because plenty main variant has to be handled first.
        $criteria->addSorting(
            new FieldSorting('itemId', FieldSorting::ASCENDING),
            new FieldSorting('isMainVariation', FieldSorting::DESCENDING),
            new FieldSorting('plentyId', FieldSorting::ASCENDING),
            ...$originalSorting
        );

        return $criteria;
    }

    protected function getPlentyUpdates(): EntityCollection
    {
        if (
            $this->debugModeEnabled === true
            && $this->debugVariationId !== null
        ) {
            return $this->getDebugPlentyUpdates();
        }

        return parent::getPlentyUpdates();
    }

    protected function getPlentyEntityIdsForLinkLoading(EntityCollection $plentyEntityUpdates): array
    {
        $plentyEntityIdsForLinkLoading = parent::getPlentyEntityIdsForLinkLoading($plentyEntityUpdates);

        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            /** @var ItemUpdateEntity $plentyEntityUpdate */

            $variationData = $plentyEntityUpdate->getVariationData();

            // ✅ Hotfix: prevent array_key_exists on null
            if (!is_array($variationData)) {
                // Optional: log once for debugging
                // $this->commonServiceRegistry->getLogger()->warning('VariationData is null/non-array', ['plentyId' => $plentyEntityUpdate->getPlentyId()]);
                continue;
            }

            $base = $variationData['base'] ?? null;

            if (
                array_key_exists('id', $variationData)
                && is_array($base)
                && array_key_exists('mainVariationId', $base)
                && $base['mainVariationId'] !== null
                && $base['mainVariationId'] !== $variationData['id']
            ) {
                $plentyEntityIdsForLinkLoading[] = $base['mainVariationId'];
            }
        }

        return $plentyEntityIdsForLinkLoading;
    }


    protected function preloadAdditionalData(EntityCollection $plentyEntityUpdates): void
    {
        $this->preloadAdditionalDataTags($plentyEntityUpdates);
        $this->preloadAdditionalDataCurrency($plentyEntityUpdates);
        $this->preloadAdditionalDataTax($plentyEntityUpdates);
        $this->preloadAdditionalDataUnits($plentyEntityUpdates);
        $this->preloadAdditionalDataAvailabilities($plentyEntityUpdates);
        $this->preloadAdditionalDataParentProducts($plentyEntityUpdates);
        $this->preloadAdditionalDataManufacturer($plentyEntityUpdates);
        $this->preloadAdditionalDataCategory($plentyEntityUpdates);
        $this->preloadAdditionalDataVariantAttributeValues($plentyEntityUpdates);
        $this->preloadAdditionalDataVariantPropertyValues($plentyEntityUpdates);
        $this->preloadAdditionalDataMedia($plentyEntityUpdates);
        $this->preloadAdditionalDataSalesChannels();
        $this->preloadAdditionalDataExistingProducts($plentyEntityUpdates);
        $this->preloadDynamicPropertyGroups($plentyEntityUpdates);
    }

    private function preloadAdditionalDataTags(EntityCollection $plentyEntityUpdates): void
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */

        if ($this->commonServiceRegistry->getSystemConfigService()->getBool('LenzPlatformPlentyConnector.config.syncTagsEnabled') !== true) {
            return;
        }

        $tagIds = [];

        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            $variationData = $plentyEntityUpdate->getVariationData();

            if (!isset($variationData['tags']) || !is_array($variationData['tags'])) {
                continue;
            }
            foreach ($variationData['tags'] as $tag) {
                if (
                    $this->tagIdRestrictions !== null
                    && !in_array($tag['tagId'], $this->tagIdRestrictions)
                ) {
                    // Skip, if tag is not allowed here.
                    continue;
                }

                $tagIds[] = $tag['tagId'];
            }
        }

        $tagIds = array_values(array_unique($tagIds));
        $this->dataStore->setValues(self::DATA_STORE_KEY_TAG_ID_2_SW_ID, $this->loadPlenty2SwMapping($tagIds, $this->commonServiceRegistry->getTagLinkRepository()));
    }

    protected function preloadAdditionalDataCurrency(EntityCollection $plentyEntityUpdates)
    {
        $currencies = $this->commonServiceRegistry->getSwCurrencyRepository()->search(new Criteria(), $this->getSwContext());
        // Set price ids for default currency.
        $this->additionalData['swCurrency2PriceIds'][Defaults::CURRENCY] = [
            'priceId' => (int) $this->commonServiceRegistry->getConfigService()->getConfigValue(ConfigService::CONFIG_KEY_PLENTY_RETAIL_PRICE_ID, 1),
            'listPriceId' => (int) $this->commonServiceRegistry->getConfigService()->getConfigValue(ConfigService::CONFIG_KEY_PLENTY_SUGGESTED_RETAIL_PRICE_ID, 2),
        ];
        foreach ($currencies as $currency) {
            /** @var CurrencyEntity $currency */
            $priceId = $currency->getCustomFieldsValue('lenz_platform_plenty_connector_currency_price_id');
            $listPriceId = $currency->getCustomFieldsValue('lenz_platform_plenty_connector_currency_list_price_id');
            if ($currency->getId() && $priceId === null) {
                // Skip default currency if no price is set in custom fields.
                continue;
            }

            $this->additionalData['swCurrency2PriceIds'][$currency->getId()] = [
                'priceId' => (int) $priceId,
                'listPriceId' => (int) $listPriceId,
            ];
        }
    }

    protected function preloadAdditionalDataTax(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $taxIds = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                array_key_exists('base', $plentyEntityUpdate->getVariationData())
                && array_key_exists('vatId', $plentyEntityUpdate->getVariationData()['base'])
                && $plentyEntityUpdate->getVariationData()['base']['vatId'] !== null
            ) {
                $taxIds[] = $plentyEntityUpdate->getVariationData()['base']['vatId'];
            }
        }

        $taxIds = array_values(array_unique($taxIds));
        $this->additionalData['plentyTaxId2SwId'] = $this->loadPlenty2SwMapping($taxIds, $this->commonServiceRegistry->getTaxLinkRepository(), function (TaxLinkEntity $link) {

            return [
                'id' => $link->getSwId(),
                'taxRate' => $link->getTaxRate(),
            ];
        });
    }

    protected function preloadAdditionalDataUnits(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                !array_key_exists('unit', $plentyEntityUpdate->getVariationData())
                || $plentyEntityUpdate->getVariationData()['unit'] === null
            ) {
                continue;
            }
            $ids[] = $plentyEntityUpdate->getVariationData()['unit']['unitId'];
        }

        $ids = array_values(array_unique($ids));
        $this->additionalData['plentyUnitId2SwId'] = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getUnitLinkRepository());
    }

    protected function preloadAdditionalDataAvailabilities(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                array_key_exists('base', $plentyEntityUpdate->getVariationData())
                && array_key_exists('availabilityId', $plentyEntityUpdate->getVariationData()['base'])
                && $plentyEntityUpdate->getVariationData()['base']['availabilityId'] !== null
            ) {
                $ids[] = $plentyEntityUpdate->getVariationData()['base']['availabilityId'];
            }
        }

        $ids = array_values(array_unique($ids));
        $this->additionalData['plentyAvailabilityId2SwId'] = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getAvailabilityLinkRepository());
    }

    protected function preloadAdditionalDataParentProducts(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                array_key_exists('base', $plentyEntityUpdate->getVariationData())
                && array_key_exists('mainVariationId', $plentyEntityUpdate->getVariationData()['base'])
                && $plentyEntityUpdate->getVariationData()['base']['mainVariationId'] !== null
                && $plentyEntityUpdate->getVariationData()['base']['mainVariationId'] !== $plentyEntityUpdate->getVariationData()['id']
            ) {
                $ids[] = $plentyEntityUpdate->getVariationData()['base']['mainVariationId'];
            }
        }

        $ids = array_values(array_unique($ids));
        $this->additionalData['plentyProductId2SwId'] = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getItemLinkRepository());
    }

    protected function preloadAdditionalDataManufacturer(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                !array_key_exists('base', $plentyEntityUpdate->getVariationData())
                || !array_key_exists('item', $plentyEntityUpdate->getVariationData()['base'])
                || !array_key_exists('manufacturerId', $plentyEntityUpdate->getVariationData()['base']['item'])
                || $plentyEntityUpdate->getVariationData()['base']['item']['manufacturerId'] === 0
            ) {
                continue;
            }
            $ids[] = $plentyEntityUpdate->getVariationData()['base']['item']['manufacturerId'];
        }

        $ids = array_values(array_unique($ids));
        $this->additionalData['plentyManufacturerId2SwId'] = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getManufacturerLinkRepository());
    }

    protected function preloadAdditionalDataCategory(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                array_key_exists('categories', $plentyEntityUpdate->getVariationData())
                && $plentyEntityUpdate->getVariationData()['categories'] !== null
            ) {
                foreach ($plentyEntityUpdate->getVariationData()['categories'] as $variationCategory) {
                    $ids[] = $variationCategory['categoryId'];
                }
            }
        }

        $ids = array_values(array_unique($ids));

        $mappingData = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getCategoryLinkRepository());
        $this->additionalData['plentyCategoryId2SwId'] = $mappingData;
        $this->dataStore->setValues(self::DATA_STORE_KEY_CATEGORY_ID_2_SW_ID, $mappingData);
    }

    private function preloadAdditionalDataVariantAttributeValues(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                array_key_exists('attributeValues', $plentyEntityUpdate->getVariationData())
                && $plentyEntityUpdate->getVariationData()['attributeValues'] !== null
            ) {
                foreach ($plentyEntityUpdate->getVariationData()['attributeValues'] as $variationAttributeValue) {
                    $ids[] = $variationAttributeValue['valueId'];
                }
            }
        }

        $ids = array_values(array_unique($ids));
        $this->additionalData['plentyAttributeValue2SwId'] = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getAttributeValueLinkRepository());
    }

    private function preloadAdditionalDataVariantPropertyValues(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (
                array_key_exists('properties', $plentyEntityUpdate->getVariationData())
                && $plentyEntityUpdate->getVariationData()['properties'] !== null
            ) {
                foreach ($plentyEntityUpdate->getVariationData()['properties'] as $property) {
                    if (
                        !in_array($property['property']['cast'], [
                            Property::PROPERTY_CAST_TYPE_SELECTION,
                            Property::PROPERTY_CAST_TYPE_MULTISELECTION,
                        ])
                    ) {
                        continue;
                    }

                    foreach ($property['values'] as $propertyValue) {
                        $ids[] = $propertyValue['value'];
                    }
                }
            }
        }

        $ids = array_values(array_unique($ids));
        $this->additionalData['plentyPropertyValue2SwId'] = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getPropertyValueLinkRepository());
    }

    protected function preloadAdditionalDataSalesChannels()
    {
        $criteria = new Criteria();
        /** @var SalesChannelCollection $salesChannels */
        $salesChannels = $this->commonServiceRegistry->getSwSalesChannelRepository()->search($criteria, $this->getSwContext())->getEntities();
        $this->additionalData['salesChannels'] = $salesChannels;
        $this->dataStore->setValues(self::DATA_STORE_KEY_SALES_CHANNELS, $salesChannels);
        $salesChannelsWithMarketCollection = new SalesChannelCollection();
        $salesChannelsWithPlentyId = new SalesChannelCollection();
        foreach ($salesChannels as $salesChannel) {
            $customFields = $salesChannel->getCustomFields() ?? [];
            if (
                array_key_exists('lenz_platform_plenty_connector_sales_channel_market_id', $customFields)
                && !empty($customFields['lenz_platform_plenty_connector_sales_channel_market_id'])
            ) {
                $salesChannelsWithMarketCollection->add($salesChannel);
            }

            if (
                array_key_exists('lenz_platform_plenty_connector_sales_channel_plenty_id', $customFields)
                && !empty($customFields['lenz_platform_plenty_connector_sales_channel_plenty_id'])
            ) {
                $salesChannelsWithPlentyId->add($salesChannel);
            }
        }
        $this->additionalData['salesChannelsWithMarket'] = $salesChannelsWithMarketCollection;
        $this->additionalData['salesChannelsWithPlentyId'] = $salesChannelsWithPlentyId;
    }

    private function preloadAdditionalDataMedia(EntityCollection $plentyEntityUpdates)
    {
        /** @var ItemUpdateCollection $plentyEntityUpdates */
        $ids = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            $images = ImageHelper::getImages($plentyEntityUpdate->getVariationData());
            foreach ($images as $image) {
                $isImageAllowed = $this->commonServiceRegistry->getImageHelper()->isImageAllowed($image, $this->getSwContext());
                if (!$isImageAllowed) {
                    // Skip image because it is not allowed here.
                    continue;
                }

                $ids[] = MediaLinkEntity::PRODUCT_MEDIA_PREFIX . $image['id'];
            }

            /** @var ItemUpdateEntity $plentyEntityUpdate */
            if (
                array_key_exists('properties', $plentyEntityUpdate->getVariationData())
                && !empty($plentyEntityUpdate->getVariationData()['properties'])
            ) {
                foreach ($plentyEntityUpdate->getVariationData()['properties'] as $property) {
                    if (
                        (
                            $this->propertyGroupIdRestrictions !== null
                            && !in_array($property['propertyId'], $this->propertyGroupIdRestrictions)
                        )
                        || (
                            $property['property']['cast'] !== 'file'
                        )
                    ) {
                        // Skip, if property is not allowed here or not of property type "file".
                        continue;
                    }

                    foreach ($property['values'] as $propertyValue) {
                        $ids[] = MediaLinkEntity::PRODUCT_PROPERTY_MEDIA_PREFIX . $propertyValue['id'];
                    }
                }
            }
        }

        $ids = array_values(array_unique($ids));
        $this->additionalData['plentyImageId2SwId'] = $this->loadPlenty2SwMapping($ids, $this->commonServiceRegistry->getMediaLinkRepository());
    }

    protected function preloadAdditionalDataExistingProducts(EntityCollection $plentyEntityUpdates)
    {
        $swIds = array_values(array_unique(array_merge(array_values($this->plentyEntityId2SwId), array_values($this->additionalData['plentyProductId2SwId']))));
        $existingProducts = new ProductCollection();
        if (!empty($swIds)) {
            $criteria = new Criteria($swIds);
            $criteria->addAssociation('categories');
            $criteria->addAssociation('properties');
            $criteria->addAssociation('visibilities');
            $criteria->addAssociation('options');
            $criteria->addAssociation('configuratorSettings');
            $criteria->addAssociation('configuratorSettings.option');
            $criteria->addAssociation('media');
            $criteria->addAssociation('mainCategories');
            $criteria->addAssociation('tags');

            /** @var CriteriaEvent $event */
            $event = $this->commonServiceRegistry->getEventDispatcher()->dispatch(new CriteriaEvent('data.process-product.additional-data.existing-products', $criteria));

            $existingProducts = $this->commonServiceRegistry->getSwProductRepository()->search($criteria, $this->getSwContext());
        }

        // Deprecated: The next line is deprecated an will be removed soon. Use data store instead.
        $this->additionalData['swExistingProducts'] = $existingProducts;
        $this->dataStore->setValues(self::DATA_STORE_KEY_EXISTING_SW_PRODUCTS, $existingProducts);
    }

    protected function preloadDynamicPropertyGroups(EntityCollection $plentyEntityUpdates): void
    {
        $this->dynamicProperties = [];
        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            /** @var ItemUpdateEntity $plentyEntityUpdate */
            if (
                !array_key_exists('properties', $plentyEntityUpdate->getVariationData())
                || empty($plentyEntityUpdate->getVariationData()['properties'])
            ) {
                continue;
            }

            foreach ($plentyEntityUpdate->getVariationData()['properties'] as $property) {
                if (
                    $this->propertyGroupIdRestrictions !== null
                    && !in_array($property['propertyId'], $this->propertyGroupIdRestrictions)
                ) {
                    // Skip, if property is not allowed here.
                    continue;
                }

                if (array_key_exists($property['propertyId'], $this->dynamicProperties)) {
                    continue;
                }

                if (
                    in_array($property['property']['cast'], [
                        'string',
                        'shortText',
                        'empty',
                        'float',
                        'int',
                        'longText',
                        'file',
                        'text',
                        'html',
                        'date',
                    ])
                ) {
                    $this->dynamicProperties[$property['propertyId']] = [
                        'cast' => $property['property']['cast'],
                    ];
                }
            }
        }

        if (empty($this->dynamicProperties)) {
            return;
        }

        $customFieldSetId = $this->getProductCustomFieldSetId();
        // Add custom fields if not existing.
        $customFieldCriteria = new Criteria();
        $customFieldNamesToSearchFor = [];
        foreach ($this->dynamicProperties as $propertyId => $property) {
            $customFieldNamesToSearchFor[] = self::CUSTOM_FIELD_PROPERTY_PREFIX . $propertyId;
        }
        $customFieldCriteria->addFilter(new EqualsAnyFilter('name', $customFieldNamesToSearchFor));
        /** @var CustomFieldCollection $customFields */
        $customFields = $this->commonServiceRegistry->getSwCustomFieldRepository()->search($customFieldCriteria, $this->getSwContext())->getEntities();
        $existingCustomFieldPropertyIds = [];
        foreach ($customFields as $customField) {
            $customFieldName = $customField->getName();
            $customFieldPropertyId = str_replace(self::CUSTOM_FIELD_PROPERTY_PREFIX, '', $customFieldName);
            $existingCustomFieldPropertyIds[] = $customFieldPropertyId;
        }

        $customFieldCreate = [];
        foreach ($this->dynamicProperties as $propertyId => $property) {
            if (in_array($propertyId, $existingCustomFieldPropertyIds)) {
                // Custom field for property already exists. Ignore.
                continue;
            }

            $currentCustomFieldsCreate = [
                'name' => self::CUSTOM_FIELD_PROPERTY_PREFIX . $propertyId,
                'type' => $this->getCustomFieldTypeByPlentymarketsCastType($property['cast']),
                'config' => [
                    'label' => [
                        'en-GB' => 'Property ' . $propertyId,
                        'de-DE' => 'Eigenschaft ' . $propertyId,
                    ],
                    'customFieldPosition' => 10000 + intval($propertyId),
                ],
                'active' => true,
                'customFieldSetId' => $customFieldSetId,
                'allowCustomerWrite' => false,
                'allowCartExpose' => false,
            ];
            if ($property['cast'] === 'file') {
                $currentCustomFieldsCreate['config']['customFieldType'] = CustomFieldTypes::MEDIA;
                $currentCustomFieldsCreate['config']['componentName'] = 'sw-media-field';
            }

            $customFieldCreate[] = $currentCustomFieldsCreate;
        }

        if (!empty($customFieldCreate)) {
            $this->commonServiceRegistry->getSwCustomFieldRepository()->create($customFieldCreate, $this->getSwContext());
        }
    }

    protected function getProductCustomFieldSetId(): string
    {
        $criteria = new Criteria();
        $criteria->addFilter(new EqualsFilter('name', 'lenz_platform_plenty_connector_product'));
        $criteria->setLimit(1);
        $customFieldSetId = $this->commonServiceRegistry->getSwCustomFieldSetRepository()->searchIds($criteria, $this->getSwContext())->firstId();
        if ($customFieldSetId === null) {
            throw new \Exception('Could not find custom field set.');
        }

        return $customFieldSetId;
    }

    protected function isPlentyEntityUpdateValid(Entity $plentyEntityUpdate): bool
    {
        /** @var ItemUpdateEntity $plentyEntityUpdate */
        if (
            $this->commonServiceRegistry->getConfigService()->isVariationMarketAllowed($this->getProductMarketIds($plentyEntityUpdate->getVariationData()), null, $this->getSwContext())
            && $this->commonServiceRegistry->getConfigService()->isVariationPlentyIdAllowed($this->getProductClientIds($plentyEntityUpdate->getVariationData()), null, $this->getSwContext())
            && $plentyEntityUpdate->getVariationData()['base']['mainVariationId'] !== null
            && $plentyEntityUpdate->getVariationData()['base']['mainVariationId'] !== $plentyEntityUpdate->getVariationData()['id']
            && !array_key_exists($plentyEntityUpdate->getVariationData()['base']['mainVariationId'], $this->plentyEntityId2SwId)
            && $plentyEntityUpdate->getErrorCount() < 1
        ) {
            $this->commonServiceRegistry->getLogger()->error(
                'Skipped variant. Variant "' . $plentyEntityUpdate->getVariationData()['id'] . '" is available, but parent is not. Please check parent variation id "' . $plentyEntityUpdate->getVariationData()['base']['mainVariationId'] . '". Please reset the sync after changing the availability of the main variant.'
            );

            return false;
        }

        return true;
    }

    protected function plentyEntity2Data($plentyManufacturerUpdate): array
    {
        return [
            'item' => $plentyManufacturerUpdate->getData(),
            'variation' => $plentyManufacturerUpdate->getVariationData(),
            '_dynamicProperties' => $this->getDynamicProperties($plentyManufacturerUpdate->getVariationData()),
        ];
    }

    private function getDynamicProperties($variationData): array
    {
        if (
            !array_key_exists('properties', $variationData)
            || empty($variationData['properties'])
        ) {
            return [];
        }

        $formattedProperties = [];
        foreach ($variationData['properties'] as $property) {
            if ($property['property']['cast'] === 'empty') {
                $property['values'] = [
                    [
                        'value' => true,
                        'lang' => 'de',
                    ]
                ];
            }

            $formattedProperties[$property['propertyId']] = $property;
        }

        return $formattedProperties;
    }

    public function getMapping(): array
    {
        $mapping = [
            (new MappingField('parentId', MappingField::TYPE_STRING, ['variation'], function ($value, $accessPath, $language, DataStore $dataStore) {
                $swId = $dataStore->getValues(self::DATA_STORE_KEY_CURRENT_MAPPING_SW_ID)[0];

                if (
                    ($value['base']['isMain'] ?? null) === false
                    && !array_key_exists(($value['base']['mainVariationId'] ?? null), $this->additionalData['plentyProductId2SwId'])
                ) {
                    // Variant is available, but parent is not.
                    return null;
                }

                if (
                    ($value['base']['isMain'] ?? null) === false
                    && ($value['base']['mainVariationId'] ?? null) !== null
                ) {
                    // Child.
                    if (!array_key_exists($value['id'], $this->additionalData['plentyProductId2SwId'])) {
                        $this->additionalData['plentyProductId2SwId'][$value['id']] = $swId;
                    }

                    return $this->additionalData['plentyProductId2SwId'][$value['base']['mainVariationId']];
                }

                // Main variation or parent not found.
                return null;
            })),
            (new MappingField('manufacturerId', MappingField::TYPE_STRING, ['variation.base.item.manufacturerId'], function ($value, $accessPath, $language, DataStore $dataStore) {
                if ($value !== 0) {
                    return $this->additionalData['plentyManufacturerId2SwId'][$value] ?? null;
                }

                return null;
            })),
            (new MappingField('description', MappingField::TYPE_STRING, ['variation.base.texts.0.description']))->setTranslatedField(true),
            (new MappingField('metaTitle', MappingField::TYPE_STRING, ['variation.base.texts.0.name']))->setTranslatedField(true),
            (new MappingField('metaDescription', MappingField::TYPE_STRING, ['variation.base.texts.0.metaDescription'], function ($value) {

                if (strlen($value) > 255) {
                    $value = substr($value, 0, 255);
                }

                return $value;
            }))->setTranslatedField(true),
            (new MappingField('keywords', MappingField::TYPE_STRING, ['variation.base.texts.0.metaKeywords'], function ($value) {

                if ($value === '') {
                    return null;
                }

                return $value;
            }))->setTranslatedField(true),
            new MappingField('productNumber', MappingField::TYPE_STRING, ['variation.base.number']),
            (new MappingField('stock', MappingField::TYPE_INT, ['variation'], function ($value, $accessPath) {

                /** @var ProductEntity|null $swExistingProduct */
                $swExistingProduct = $this->getSwExistingProductByPlentyId($value['id']);
                if ($swExistingProduct !== null) {
                    // Only set stock value on product creation.
                    return $swExistingProduct->getStock();
                }

                $allowedWarehouseIdsFallback = json_decode($this->commonServiceRegistry->getConfigService()->getConfigValue(ConfigService::CONFIG_KEY_WAREHOUSE_TYPE_SALES, '[]'), true);
                $sAllowedWarehouseIds = $this->commonServiceRegistry->getConfigService()->getConfigValue(ConfigService::CONFIG_KEY_PLENTY_WAREHOUSE_IDS) ?? '';
                $sAllowedWarehouseIds = str_replace(' ', '', $sAllowedWarehouseIds);
                $aAllowedWarehouseIds = explode(',', $sAllowedWarehouseIds);
                $stock = 0;
                if (
                    array_key_exists('base', $value)
                    && array_key_exists('stock', $value['base'])
                    && $value['base']['stock'] !== null
                ) {
                    foreach ($value['base']['stock'] as $item) {
                        if ($value['id'] != $item['variationId']) {
                            continue;
                        }

                        if (
                            (
                                empty($sAllowedWarehouseIds)
                                && in_array($item['warehouseId'], $allowedWarehouseIdsFallback)
                            )
                            || in_array($item['warehouseId'], $aAllowedWarehouseIds)
                        ) {
                            $stock += $item['stockPhysical'] - $item['reservedStock'] - $item['reservedBundle'];
                        }
                    }
                }

                return $stock;
            }))
                ->setUnsetNullValue(true)
            ,
            new MappingField('purchasePrices', MappingField::TYPE_ARRAY, ['variation'], function ($value, $accessPath) {

                if (empty($value['base']['purchasePrice'] ?? null)) {
                    return null;
                }

                $purchasePriceNet = $value['base']['purchasePrice'];
                $taxRate = $this->additionalData['plentyTaxId2SwId'][$value['base']['vatId']]['taxRate'];
                $purchasePrice = [
                    'c' . Defaults::CURRENCY => [
                        'currencyId' => Defaults::CURRENCY,
                        'net' => round($purchasePriceNet, 9),
                        'linked' => true,
                        'gross' => round($purchasePriceNet * (1 + $taxRate / 100), 9),
                    ]
                ];
                return $purchasePrice;
            }),
            new MappingField('releaseDate', MappingField::TYPE_DATETIME, ['variation.base.releasedAt'], function ($value, $accessPath) {

                if ($value === '') {
                    return null;
                }

                return $value;
            }),
            new MappingField('ean', MappingField::TYPE_STRING, ['variation.barcodes'], function ($value, $accessPath) {

                foreach ($value as $barcode) {
                    if ($barcode['barcodeId'] == 1) {
                        return $barcode['code'];
                    }
                }

                return null;
            }),
            new MappingField('active', MappingField::TYPE_BOOLEAN, ['variation.base.isActive']),
            new MappingField('weight', MappingField::TYPE_FLOAT, ['variation.base.weightG'], function ($value, $accessPath) {

                if ($value === '') {
                    return null;
                }
                return $value / 1000;
            }),
            new MappingField('width', MappingField::TYPE_FLOAT, ['variation.base.widthMM']),
            new MappingField('height', MappingField::TYPE_FLOAT, ['variation.base.heightMM']),
            new MappingField('length', MappingField::TYPE_FLOAT, ['variation.base.lengthMM']),
            new MappingField('purchaseUnit', MappingField::TYPE_FLOAT, ['variation.unit.content']),
            new MappingField('minPurchase', MappingField::TYPE_INT, ['variation.base.minimumOrderQuantity'], function ($value, $accessPath) {

                if ($value < 1) {
                    $value = 1;
                }
                return $value;
            }),
            new MappingField('maxPurchase', MappingField::TYPE_INT, ['variation.base.maximumOrderQuantity'], function ($value, $accessPath) {

                if ($value < 1) {
                    $value = 100;
                }
                return $value;
            }),
            // TODO: Wiederauffüllzeit in Tagen
            // TODO: Staffelung
            // TODO: Versandkostenfrei
            new MappingField('manufacturerNumber', MappingField::TYPE_STRING, ['variation.supplier'], function ($value, $accessPath) {

                foreach ($value as $item) {
                    if (!empty($item['itemNumber'])) {
                        return $item['itemNumber'];
                    }
                }
                return null;
            }),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_plenty_item_id', MappingField::TYPE_INT, ['variation.base.itemId']))
                ->setTranslatedField(true)
            ,
            (new MappingField('customFields.lenz_platform_plenty_connector_product_plenty_variation_id', MappingField::TYPE_INT, ['variation.id']))
                ->setTranslatedField(true)
            ,
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free1', MappingField::TYPE_STRING, ['variation.base.item.free1']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free2', MappingField::TYPE_STRING, ['variation.base.item.free2']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free3', MappingField::TYPE_STRING, ['variation.base.item.free3']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free4', MappingField::TYPE_STRING, ['variation.base.item.free4']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free5', MappingField::TYPE_STRING, ['variation.base.item.free5']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free6', MappingField::TYPE_STRING, ['variation.base.item.free6']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free7', MappingField::TYPE_STRING, ['variation.base.item.free7']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free8', MappingField::TYPE_STRING, ['variation.base.item.free8']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free9', MappingField::TYPE_STRING, ['variation.base.item.free9']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free10', MappingField::TYPE_STRING, ['variation.base.item.free10']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free11', MappingField::TYPE_STRING, ['variation.base.item.free11']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free12', MappingField::TYPE_STRING, ['variation.base.item.free12']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free13', MappingField::TYPE_STRING, ['variation.base.item.free13']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free14', MappingField::TYPE_STRING, ['variation.base.item.free14']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free15', MappingField::TYPE_STRING, ['variation.base.item.free15']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free16', MappingField::TYPE_STRING, ['variation.base.item.free16']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free17', MappingField::TYPE_STRING, ['variation.base.item.free17']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free18', MappingField::TYPE_STRING, ['variation.base.item.free18']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free19', MappingField::TYPE_STRING, ['variation.base.item.free19']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_free20', MappingField::TYPE_STRING, ['variation.base.item.free20']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_url_path', MappingField::TYPE_STRING, ['variation.base.texts.0.urlPath']))
                ->setTranslatedField(true),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_technical_data', MappingField::TYPE_STRING, ['variation.base.texts.0.technicalData']))
                ->setTranslatedField(true),
            new MappingField('deliveryTimeId', MappingField::TYPE_STRING, ['variation.base.availabilityId'], function ($value) {

                if (array_key_exists($value, $this->additionalData['plentyAvailabilityId2SwId'])) {
                    return $this->additionalData['plentyAvailabilityId2SwId'][$value];
                }

                return null;
            }),
            new MappingField('categories', MappingField::TYPE_ARRAY, ['variation'], function ($value, $accessPath) {
                $swExistingProduct = $this->getSwExistingProductByPlentyId($value['id']);
                // Category.
                $categoryIdsToCreate = [];

                foreach (($value['categories'] ?? []) as $variationCategory) {
                    if (!array_key_exists($variationCategory['categoryId'], $this->additionalData['plentyCategoryId2SwId'])) {
                        // Ignore categories that are not existing in Shopware.
                        continue;
                    }
                    $categoryIdsToCreate[] = $this->additionalData['plentyCategoryId2SwId'][$variationCategory['categoryId']];
                }

                if ($swExistingProduct !== null && $swExistingProduct->getCategoryIds() !== null) {
                    foreach ($swExistingProduct->getCategoryIds() as $swCategoryId) {
                        if (!in_array($swCategoryId, $categoryIdsToCreate)) {
                            // Delete.
                            $this->productCategoryDelete[] = [
                                'productId' => $swExistingProduct->getId(),
                                'categoryId' => $swCategoryId,
                            ];
                        }

                        // Don't add same option twice.
                        $key = array_search($swCategoryId, $categoryIdsToCreate);
                        if ($key !== false) {
                            unset($categoryIdsToCreate[$key]);
                        }
                    }
                }

                $categoryAssignments = [];
                foreach ($categoryIdsToCreate as $categoryId) {
                    $categoryAssignments[] = ['id' => $categoryId];
                }
                return $categoryAssignments;
            }),
            (new MappingField('customFields.lenz_platform_plenty_connector_product_last_updated_at', MappingField::TYPE_STRING, []))
                ->setTranslatedField(true)
                ->setDefaultValue((new \DateTimeImmutable())->format('Y-m-d H:i:s'))
            ,
            (new MappingField('taxId', MappingField::TYPE_STRING, ['variation.base.vatId'], function ($value) {

                return $this->additionalData['plentyTaxId2SwId'][$value]['id'];
            })),
            (new MappingField('price', MappingField::TYPE_ARRAY, ['variation'], function ($value) {
                $priceArray = [];
                foreach ($this->additionalData['swCurrency2PriceIds'] as $swCurrencyId => $priceIds) {
                    $price = $this->getPriceByPriceId($priceIds['priceId'], $priceIds['listPriceId'], $value, $swCurrencyId);
                    if ($price !== null) {
                        $priceArray[] = $price;
                    }
                }

                return $priceArray;
            })),
            (new MappingField('unitId', MappingField::TYPE_STRING, ['variation.unit.unitId'], function ($value) {
                $unitId = null;

                if (array_key_exists($value, $this->additionalData['plentyUnitId2SwId'])) {
                    $unitId = $this->additionalData['plentyUnitId2SwId'][$value];
                }

                return $unitId;
            })),
            (new MappingField('tags', MappingField::TYPE_ARRAY, ['variation'], function (mixed $value, string $accessPath, LanguageEntity $language, DataStore $dataStore) {
                if ($this->commonServiceRegistry->getSystemConfigService()->getBool('LenzPlatformPlentyConnector.config.syncTagsEnabled') !== true) {
                    return [];
                }

                /** @var ?ProductEntity $swExistingProduct */
                $swExistingProduct = $dataStore->getValue(self::DATA_STORE_KEY_EXISTING_SW_PRODUCTS, $dataStore->getValue(self::DATA_STORE_KEY_CURRENT_MAPPING_SW_ID, 0));
                $tagIdsToCreate = [];

                if (!empty($value['tags'])) {
                    foreach ($value['tags'] as $variationTag) {
                        if (!$dataStore->hasValue(self::DATA_STORE_KEY_TAG_ID_2_SW_ID, (string) $variationTag['tagId'])) {
                            // Ignore tags that are not existing in Shopware.
                            continue;
                        }

                        $tagIdsToCreate[] = $dataStore->getValue(self::DATA_STORE_KEY_TAG_ID_2_SW_ID, $variationTag['tagId']);
                    }
                }

                foreach (($swExistingProduct?->getTags()?->getIds() ?? []) as $swTagId) {
                    if (!in_array($swTagId, $tagIdsToCreate)) {
                        // Delete.
                        $dataStore->addValue(
                            self::DATA_STORE_KEY_PRODUCT_TAG_DELETE,
                            Uuid::randomHex(),
                            [
                                'productId' => $swExistingProduct->getId(),
                                'tagId' => $swTagId,
                            ]
                        );
                    }

                    $key = array_search($swTagId, $tagIdsToCreate);
                    if ($key !== false) {
                        unset($tagIdsToCreate[$key]);
                    }
                }

                $tagAssignments = [];

                foreach ($tagIdsToCreate as $tagId) {
                    $tagAssignments[] = ['id' => $tagId];
                }
                return $tagAssignments;
            })),
            (new MappingField('visibilities', MappingField::TYPE_ARRAY, ['variation'], function ($value) {
                /** @var ProductEntity|null $swExistingProduct */
                $swExistingProduct = $this->getSwExistingProductByPlentyId($value['id']);

                $visibilities = [];
                $salesChannelIdsToCreate = [];
                if (
                    $this->additionalData['salesChannelsWithMarket']->count() === 0
                    && $this->additionalData['salesChannelsWithPlentyId']->count() === 0
                ) {
                    $salesChannelIdsToCreate = $this->additionalData['salesChannels']->getIds();
                } else {
                    /** @var SalesChannelEntity $salesChannel */
                    foreach ($this->additionalData['salesChannelsWithMarket'] as $salesChannel) {
                        $customFields = $salesChannel->getCustomFields() ?? [];
                        if (
                            array_key_exists('lenz_platform_plenty_connector_sales_channel_market_id', $customFields)
                            && !empty($customFields['lenz_platform_plenty_connector_sales_channel_market_id'])
                            && $this->commonServiceRegistry->getConfigService()->isVariationMarketAllowed($this->getProductMarketIds($value), [$customFields['lenz_platform_plenty_connector_sales_channel_market_id']], $this->getSwContext())
                            && !empty($customFields['lenz_platform_plenty_connector_sales_channel_plenty_id'])
                            && $this->commonServiceRegistry->getConfigService()->isVariationPlentyIdAllowed($this->getProductClientIds($value), [$customFields['lenz_platform_plenty_connector_sales_channel_plenty_id']], $this->getSwContext())
                        ) {
                            $salesChannelIdsToCreate[] = $salesChannel->getId();
                        }
                    }
                }

                if ($swExistingProduct !== null) {
                    /** @var ProductVisibilityEntity $swProductVisibility */
                    foreach ($swExistingProduct->getVisibilities() as $swProductVisibility) {
                        if (!in_array($swProductVisibility->getSalesChannelId(), $salesChannelIdsToCreate)) {
                            // Delete.
                            $this->productVisibilityDelete[] = [
                                'id' => $swProductVisibility->getId(),
                                'productId' => $swExistingProduct->getId(),
                                'salesChannelId' => $swProductVisibility->getSalesChannelId(),
                            ];
                        }

                        // Don't add same option twice.
                        $key = array_search($swProductVisibility->getSalesChannelId(), $salesChannelIdsToCreate);
                        if ($key !== false) {
                            unset($salesChannelIdsToCreate[$key]);
                        }
                    }
                }
                foreach ($salesChannelIdsToCreate as $salesChannelId) {
                    $visibilities[] = [
                        'salesChannelId' => $salesChannelId,
                        'visibility' => ProductVisibilityDefinition::VISIBILITY_ALL,
                    ];
                }

                return $visibilities;
            })),
            (new MappingField('properties', MappingField::TYPE_ARRAY, ['variation'], function ($value) {
                /** @var ProductEntity|null $swExistingProduct */
                $swExistingProduct = $this->getSwExistingProductByPlentyId($value['id']);

                // Plenty variation attribute + plenty item merkmal = Shopware property ids.
                $properties = [];
                $variantPropertyIdsToCreate = [];
                if (!empty($value['attributeValues'])) {
                    foreach ($value['attributeValues'] as $variationAttributeValue) {
                        if (!array_key_exists($variationAttributeValue['valueId'], $this->additionalData['plentyAttributeValue2SwId'])) {
                            continue;
                        }
                        $variantPropertyIdsToCreate[] = $this->additionalData['plentyAttributeValue2SwId'][$variationAttributeValue['valueId']];
                    }
                }
                if (!empty($value['properties'])) {
                    foreach ($value['properties'] as $property) {
                        if (
                            !in_array($property['property']['cast'], [
                                Property::PROPERTY_CAST_TYPE_SELECTION,
                                Property::PROPERTY_CAST_TYPE_MULTISELECTION,
                            ])
                        ) {
                            continue;
                        }

                        foreach ($property['values'] as $propertyValue) {
                            if (array_key_exists($propertyValue['value'], $this->additionalData['plentyPropertyValue2SwId'])) {
                                $variantPropertyIdsToCreate[] = $this->additionalData['plentyPropertyValue2SwId'][$propertyValue['value']];
                            }
                        }
                    }
                }

                if ($swExistingProduct !== null) {
                    foreach ($swExistingProduct->getProperties()->getIds() as $swPropertyId) {
                        if (!in_array($swPropertyId, $variantPropertyIdsToCreate)) {
                            // Delete.
                            $this->productPropertyDelete[] = [
                                'productId' => $swExistingProduct->getId(),
                                'optionId' => $swPropertyId,
                            ];
                        }

                        // Don't add same option twice.
                        $key = array_search($swPropertyId, $variantPropertyIdsToCreate);
                        if ($key !== false) {
                            unset($variantPropertyIdsToCreate[$key]);
                        }
                    }
                }
                foreach ($variantPropertyIdsToCreate as $optionId) {
                    $properties[] = ['id' => $optionId];
                }

                return $properties;
            })),
            (new MappingField('media', MappingField::TYPE_ARRAY, ['variation'], function ($value, $accessPath, $language, DataStore $dataStore) {
                /** @var ProductEntity|null $swExistingProduct */
                $swExistingProduct = $this->getSwExistingProductByPlentyId($value['id']);
                $productMediaAssignmentsToCreate = [];
                $images = ImageHelper::getImages($value);

                $coverId = null;
                $fallbackCoverId = null;
                $fallbackCoverPosition = null;
                $newPosition = 0;

                if (!empty($images)) {
                    // Sort images by position.
                    usort($images, function ($a, $b) {
                        return $a['position'] <=> $b['position'];
                    });

                    foreach ($images as $itemImage) {
                        $isImageAllowed = $this->commonServiceRegistry->getImageHelper()->isImageAllowed($itemImage, $this->getSwContext());
                        if (!$isImageAllowed) {
                            // Skip image because it is not allowed here.
                            continue;
                        }

                        if (!array_key_exists($itemImage['id'], $this->additionalData['plentyImageId2SwId'])) {
                            // Skip images that are not uploaded at the moment.
                            continue;
                        }

                        $relationId = $this->generateProductMediaRelationId($dataStore->getValues(self::DATA_STORE_KEY_CURRENT_MAPPING_SW_ID)[0], $this->additionalData['plentyImageId2SwId'][$itemImage['id']], $itemImage['position']);
                        $productMediaAssignmentsToCreate[$relationId] = [
                            'id' => $relationId,
                            'media' => [
                                'id' => $this->additionalData['plentyImageId2SwId'][$itemImage['id']],
                            ],
                            'position' => $newPosition++,
                        ];

                        // Set first available image as fallback cover.
                        if (
                            $fallbackCoverId === null
                            || $fallbackCoverPosition > $itemImage['position']
                        ) {
                            $fallbackCoverId = $relationId;
                            $fallbackCoverPosition = $itemImage['position'];
                        }

                        if ($itemImage['position'] === 0) {
                            $coverId = $relationId;
                        }
                    }
                }

                if ($swExistingProduct !== null && $swExistingProduct->getMedia() !== null) {
                    foreach ($swExistingProduct->getMedia() as $mediaAssignment) {
                        if (!in_array($mediaAssignment->getId(), array_keys($productMediaAssignmentsToCreate))) {
                            // Delete.
                            $this->productMediaDelete[] = [
                                'id' => $mediaAssignment->getId(),
                                'productId' => $swExistingProduct->getId(),
                                'mediaId' => $mediaAssignment->getMediaId(),
                            ];
                        }

                        // Don't add same option twice.
                        if (array_key_exists($mediaAssignment->getId(), $productMediaAssignmentsToCreate)) {
                            unset($productMediaAssignmentsToCreate[$mediaAssignment->getId()]);
                        }
                    }
                }

                $dataStore->setValues(self::DATA_STORE_KEY_CURRENT_SW_COVER_ID, [$coverId ?? $fallbackCoverId]);

                return array_values($productMediaAssignmentsToCreate);
            })),
            (new MappingField('coverId', MappingField::TYPE_STRING, ['variation'], function ($value, $accessPath, $language, DataStore $dataStore) {
                return $dataStore->getValues(self::DATA_STORE_KEY_CURRENT_SW_COVER_ID)[0] ?? null;
            })),
            (new MappingField('options', MappingField::TYPE_ARRAY, ['variation'], function ($value) {
                /** @var ProductEntity|null $swExistingProduct */
                $swExistingProduct = $this->getSwExistingProductByPlentyId($value['id']);

                // Plenty variation attribute = Shopware option ids.
                $options = [];
                $variantOptionsIdsToCreate = [];
                if (!empty($value['attributeValues'])) {
                    foreach ($value['attributeValues'] as $variationAttributeValue) {
                        if (!array_key_exists($variationAttributeValue['valueId'], $this->additionalData['plentyAttributeValue2SwId'])) {
                            continue;
                        }
                        $variantOptionsIdsToCreate[] = $this->additionalData['plentyAttributeValue2SwId'][$variationAttributeValue['valueId']];
                    }
                }

                if ($swExistingProduct !== null) {
                    foreach ($swExistingProduct->getOptions()->getIds() as $swOptionId) {
                        if (!in_array($swOptionId, $variantOptionsIdsToCreate)) {
                            // Delete.
                            $this->productOptionDelete[] = [
                                'productId' => $swExistingProduct->getId(),
                                'optionId' => $swOptionId,
                            ];
                        }

                        // Don't add same option twice.
                        $key = array_search($swOptionId, $variantOptionsIdsToCreate);
                        if ($key !== false) {
                            unset($variantOptionsIdsToCreate[$key]);
                        }
                    }
                }
                foreach ($variantOptionsIdsToCreate as $optionId) {
                    $options[] = ['id' => $optionId];
                }

                return $options;
            })),
            (new MappingField('referenceUnit', MappingField::TYPE_INT, []))
                ->setDefaultValue(1)
            ,
            (new MappingField('_delete_configurator_settings_in_product_parent', MappingField::TYPE_STRING, ['variation'], function ($value, $accessPath, $language, DataStore $dataStore) {
                /** @var ProductEntity|null $swExistingProduct */
                $swExistingProduct = $this->getSwExistingProductByPlentyId($value['id']);

                // Delete configuratorSettings on parent product.
                if (
                    $swExistingProduct !== null
                    && ($value['base']['isMain'] ?? null) === true
                ) {
                    foreach ($swExistingProduct->getConfiguratorSettings() as $configuratorSetting) {
                        $this->productConfiguratorSettingsDelete[] = [
                            'id' => $configuratorSetting->getId(),// TODO: Check if this line can be removed from SW 6.4.10.1.
                            'productId' => $swExistingProduct->getId(),
                            'optionId' => $configuratorSetting->getOptionId(),
                        ];
                    }
                }

                return null;
            }))
                ->setUnsetNullValue(true)
            ,
            (new MappingField('_add_configurator_settings_for_all_variants', MappingField::TYPE_STRING, ['variation'], function ($value, $accessPath, $language, DataStore $dataStore) {
                // Add configurator settings for all variants that are created.
                if (($value['base']['isMain'] ?? null) === false) {
                    foreach ($value['attributeValues'] as $variationAttributeValue) {
                        if (
                            !array_key_exists($variationAttributeValue['valueId'], $this->additionalData['plentyAttributeValue2SwId'])
                            || !array_key_exists($value['base']['mainVariationId'], $this->additionalData['plentyProductId2SwId'])
                        ) {
                            continue;
                        }

                        $swParentId = $this->additionalData['plentyProductId2SwId'][$value['base']['mainVariationId']];
                        $swOptionId = $this->additionalData['plentyAttributeValue2SwId'][$variationAttributeValue['valueId']];
                        $this->productConfiguratorSettingsCreate[$swParentId][] = $swOptionId;
                    }
                }

                return null;
            }))
                ->setUnsetNullValue(true)
            ,
        ];
        // Add mappings from database.
        $mappingFields = $this->commonServiceRegistry->getConfigService()->getMappingFieldsByType(MappingFieldDefinition::TYPE_PLENTY2SW_PRODUCT);
        foreach ($mappingFields as $mappingField) {
            // Remove existing mapping.
            foreach ($mapping as $mappingKey => $mappingValue) {
                /** @var MappingField $mappingValue */
                if ($mappingValue->getSwFieldName() === $mappingField->getSwField()) {
                    unset($mapping[$mappingKey]);
                }
            }

            if ($mappingField->getPlentymarketsField() === 'variation.stockLimitation') {
                $mappingField->setPlentymarketsField('variation.base.stockLimitation');
            }

            if ($mappingField->getPlentymarketsField() === 'variation.isUnavailableIfNetStockIsNotPositive') {
                $mappingField->setPlentymarketsField('variation.base.isUnavailableIfNetStockIsNotPositive');
            }

            // Add new mapping.
            $fieldType = MappingField::TYPE_STRING;
            $callable = null;
            if ($mappingField->getSwField() == 'isCloseout') {
                $fieldType = MappingField::TYPE_BOOLEAN;
                if ($mappingField->getPlentymarketsField() === 'variation.base.stockLimitation') {
                    $callable = function ($value) {

                        $value = intval($value);
                        if ($value === 1) {
                            return true;
                        }

                        return false;
                    };
                }
            } elseif ($mappingField->getSwField() === 'mainCategories') {
                $fieldType = MappingField::TYPE_ARRAY;
                $mappingField->setPlentymarketsField('variation');
                $callable = function (mixed $value, string $accessPath, LanguageEntity $language, DataStore $dataStore) {
                    /** @var ?ProductEntity $existingProduct */
                    $existingProduct = $dataStore->getValue(
                        self::DATA_STORE_KEY_EXISTING_SW_PRODUCTS,
                        $dataStore->getValue(self::DATA_STORE_KEY_CURRENT_MAPPING_SW_ID, 0, '')
                    );

                    $categoryIdWithLowestPosition = null;
                    $position = null;
                    foreach (($value['categories'] ?? []) as $category) {
                        if (
                            // Ensure, that this category exists in Shopware.
                            $dataStore->getValue(self::DATA_STORE_KEY_CATEGORY_ID_2_SW_ID, $category['categoryId']) !== null
                            && (
                                $categoryIdWithLowestPosition === null
                                || $category['position'] < $position
                            )
                        ) {
                            $categoryIdWithLowestPosition = $dataStore->getValue(self::DATA_STORE_KEY_CATEGORY_ID_2_SW_ID, $category['categoryId']);
                            $position = $category['position'];
                        }
                    }

                    $mandante2SwCategoryId = [];
                    foreach (($value['defaultCategories'] ?? []) as $defaultCategory) {
                        $swCategoryId = $dataStore->getValue(self::DATA_STORE_KEY_CATEGORY_ID_2_SW_ID, $defaultCategory['branchId']);
                        $mandante2SwCategoryId[$defaultCategory['plentyId']] = $swCategoryId ?? $categoryIdWithLowestPosition;
                    }

                    /** @var ?SalesChannelCollection $salesChannels */
                    $salesChannels = $dataStore->getValues(self::DATA_STORE_KEY_SALES_CHANNELS);
                    $salesChannelId2SwCategoryId = [];
                    foreach ($salesChannels as $salesChannel) {
                        $salesChannelMandanteId = $salesChannel->getTranslation('customFields')['lenz_platform_plenty_connector_sales_channel_plenty_id'] ?? null;
                        $salesChannelId2SwCategoryId[$salesChannel->getId()] = $mandante2SwCategoryId[$salesChannelMandanteId] ?? $categoryIdWithLowestPosition;
                    }

                    $mainCategoryData = [];
                    foreach (($existingProduct?->getMainCategories() ?? []) as $mainCategory) {
                        if (($salesChannelId2SwCategoryId[$mainCategory->getSalesChannelId()] ?? null) === null) {
                            // Remove existing main category.
                            $dataStore->addValue(
                                self::DATA_STORE_KEY_PRODUCT_MAIN_CATEGORY_DELETE,
                                Uuid::randomHex(),
                                [
                                    'id' => $mainCategory->getId(),
                                ]
                            );
                        } else {
                            // Update existing main category.
                            $mainCategoryData[] = [
                                'id' => $mainCategory->getId(),
                                'categoryId' => $salesChannelId2SwCategoryId[$mainCategory->getSalesChannelId()],
                                'categoryVersionId' => Defaults::LIVE_VERSION,
                            ];
                        }

                        unset($salesChannelId2SwCategoryId[$mainCategory->getSalesChannelId()]);
                    }

                    foreach ($salesChannelId2SwCategoryId as $salesChannelId => $swCategoryId) {
                        $mainCategoryData[] = [
                            'categoryId' => $swCategoryId,
                            'categoryVersionId' => Defaults::LIVE_VERSION,
                            'salesChannelId' => $salesChannelId,
                        ];
                    }

                    return $mainCategoryData;
                };
            }

            $mapping[] = new MappingField($mappingField->getSwField(), $fieldType, explode(',', $mappingField->getPlentymarketsField()), $callable);
        }

        if ($this->commonServiceRegistry->getSystemConfigService()->get('LenzPlatformPlentyConnector.config.productMappingProductName') === 'variation.name') {
            $productNameAccessPaths = ['variation.base.name'];
        } elseif ($this->commonServiceRegistry->getSystemConfigService()->get('LenzPlatformPlentyConnector.config.productMappingProductName') === 'item.name2') {
            $productNameAccessPaths = ['variation.base.texts.0.name2'];
        } elseif ($this->commonServiceRegistry->getSystemConfigService()->get('LenzPlatformPlentyConnector.config.productMappingProductName') === 'item.name3') {
            $productNameAccessPaths = ['variation.base.texts.0.name3'];
        } else {
            $productNameAccessPaths = ['variation.base.texts.0.name'];
        }

        $mapping[] = (new MappingField('name', MappingField::TYPE_STRING, $productNameAccessPaths, function ($value, $accessPath) {

            if ($accessPath === 'variation.base.name' && empty($value)) {
                // Throw exception to use next access path.
                throw new SkipMappingException('Throw exception to use next access path. Don\'t worry about this exception =)');
            }

            // Prevent error for empty value.
            if (empty($value)) {
                return '-';
            }

            return $value;
        }))
            ->setTranslatedField(true)
            ->setDefaultValue('-')
        ;

        /** @var array<mixed> $dynamicProperty */
        foreach ($this->dynamicProperties as $propertyId => $dynamicProperty) {
            $swMappingType = MappingField::TYPE_STRING;
            if ($dynamicProperty['cast'] === 'float') {
                $swMappingType = MappingField::TYPE_FLOAT;
            } elseif ($dynamicProperty['cast'] === 'int') {
                $swMappingType = MappingField::TYPE_INT;
            }

            if ($dynamicProperty['cast'] === 'file') {
                $mapping[] = (new MappingField('customFields.' . self::CUSTOM_FIELD_PROPERTY_PREFIX . $propertyId, $swMappingType, ['_dynamicProperties.' . $propertyId . '.values'], function ($value) {

                    $value = $value[0]['id'];
                    if (array_key_exists(MediaLinkEntity::PRODUCT_PROPERTY_MEDIA_PREFIX . $value, $this->additionalData['plentyImageId2SwId'])) {
                        return $this->additionalData['plentyImageId2SwId'][MediaLinkEntity::PRODUCT_PROPERTY_MEDIA_PREFIX . $value];
                    }

                    return null;
                }))
                    ->setTranslatedField(true)
                ;
            } else {
                $mapping[] = (
                    new MappingField(
                        'customFields.' . self::CUSTOM_FIELD_PROPERTY_PREFIX . $propertyId,
                        $swMappingType,
                        ['_dynamicProperties.' . $propertyId . '.values.0.value'],
                        function ($value) use ($swMappingType) {
                            if ($swMappingType === MappingField::TYPE_STRING) {
                                return (string) $value;
                            }

                            if ($swMappingType === MappingField::TYPE_FLOAT) {
                                return (float) $value;
                            }

                            /** @phpstan-ignore identical.alwaysTrue */
                            if ($swMappingType === MappingField::TYPE_INT) {
                                return (int) $value;
                            }

                            /** @phpstan-ignore deadCode.unreachable */
                            if ($swMappingType === MappingField::TYPE_BOOLEAN) {
                                return (bool) $value;
                            }

                            if ($swMappingType === MappingField::TYPE_DATETIME) {
                                return (string) $value;
                            }

                            return $value;
                        }
                    )
                )
                    ->setTranslatedField(true)
                ;
            }
        }

        return $mapping;
    }

    protected function getPriceByPriceId(int $priceId, int $listPriceId, $value, $swCurrencyId): ?array
    {
        if (!array_key_exists('salesPrices', $value)) {
            return null;
        }

        // Tax.
        $taxRate = $this->additionalData['plentyTaxId2SwId'][$value['base']['vatId']]['taxRate'];
        // Price.
        $price = null;
        $listPrice = null;
        foreach ($value['salesPrices'] as $salesPrice) {
            if ((int) $salesPrice['salesPriceId'] === $priceId) {
                // VK.
                $price = [
                    'currencyId' => $swCurrencyId,
                    'net' => (float) $salesPrice['price']  / (($taxRate / 100) + 1),
                    'gross' => (float) $salesPrice['price'],
                    'linked' => true,
                ];
            } elseif ((int) $salesPrice['salesPriceId'] === $listPriceId) {
                // UVP.
                $listPrice = [
                    'currencyId' => $swCurrencyId,
                    'net' => (float) $salesPrice['price'] / (($taxRate / 100) + 1),
                    'gross' => (float) $salesPrice['price'],
                    'linked' => true,
                ];
            }
        }

        if ($price !== null) {
            if ($listPrice !== null) {
                $price['listPrice'] = $listPrice;
            }

            return $price;
        }

        if ($swCurrencyId === Defaults::CURRENCY) {
            // Fallback.
            return [
                'currencyId' => $swCurrencyId,
                'net' => (float) 0,
                'gross' => (float) 0,
                'linked' => true,
            ];
        }

        return null;
    }

    protected function manipulateConvertedEntity(array $convertedEntity, Entity $plentyEntityUpdate): ?array
    {
        /** @var ItemUpdateEntity $plentyEntityUpdate */
        if (
            ($plentyEntityUpdate->getVariationData()['base']['isMain'] ?? null) === false
            && !array_key_exists(($plentyEntityUpdate->getVariationData()['base']['mainVariationId'] ?? null), $this->additionalData['plentyProductId2SwId'])
        ) {
            // Variant is available, but parent is not.
            $this->processedPlentyEntityUpdatePlentyIds[] = $plentyEntityUpdate->getPlentyId();
            if (array_key_exists($plentyEntityUpdate->getPlentyId(), $this->plentyEntityLinkUpsertData)) {
                // Prevent connector from adding links for this product.
                unset($this->plentyEntityLinkUpsertData[$plentyEntityUpdate->getPlentyId()]);
            }

            if ($this->additionalData['swExistingProducts']->has($convertedEntity['id'])) {
                // Disable existing product.
                return [
                    'id' => $convertedEntity['id'],
                    'name' => 'MISSING_PARENT',
                    'active' => false,
                ];
            } else {
                // Returning null prevents product from being created.
                return null;
            }
        } elseif (
            $this->commonServiceRegistry->getConfigService()->isVariationMarketAllowed($this->getProductMarketIds($plentyEntityUpdate->getVariationData()), null, $this->getSwContext()) === false
            || $this->commonServiceRegistry->getConfigService()->isVariationPlentyIdAllowed($this->getProductClientIds($plentyEntityUpdate->getVariationData()), null, $this->getSwContext()) === false
            || $plentyEntityUpdate->getErrorCount() >= 100
        ) {
            $this->processedPlentyEntityUpdatePlentyIds[] = $plentyEntityUpdate->getPlentyId();
            if (array_key_exists($plentyEntityUpdate->getPlentyId(), $this->plentyEntityLinkUpsertData)) {
                // Prevent connector from adding links for this product.
                unset($this->plentyEntityLinkUpsertData[$plentyEntityUpdate->getPlentyId()]);
            }

            if ($this->additionalData['swExistingProducts']->has($convertedEntity['id'])) {
                // If product exists.
                // TODO: Delete product.

                // Disable product.
                return [
                    'id' => $convertedEntity['id'],
                    'name' => 'DELETED_PRODUCT',
                    'active' => false,
                ];
            }

            // Returning null prevents product from being updated.
            return null;
        }

        return $convertedEntity;
    }

    private function getProductMarketIds(array $variationData): array
    {
        $productMarketIds = [];
        if (array_key_exists('markets', $variationData)) {
            foreach ($variationData['markets'] as $variationMarket) {
                $productMarketIds[] = (float) $variationMarket['marketId'];
            }
        }

        return $productMarketIds;
    }

    private function getProductClientIds(array $variationData): array
    {
        $productClientIds = [];
        if (array_key_exists('clients', $variationData)) {
            foreach ($variationData['clients'] as $variationClient) {
                $productClientIds[] = (int) $variationClient['plentyId'];
            }
        }

        return $productClientIds;
    }

    protected function runBeforeUpdates(): void
    {
        if (!empty($this->productMediaDelete)) {
            $this->commonServiceRegistry->getSwProductMediaRepository()->delete($this->productMediaDelete, $this->getSwContext());
        }

        if (!empty($this->productVisibilityDelete)) {
            $this->commonServiceRegistry->getSwProductVisibilityRepository()->delete($this->productVisibilityDelete, $this->getSwContext());
        }

        // Delete outdated product main category assignment.
        if ($this->dataStore->hasValues(self::DATA_STORE_KEY_PRODUCT_MAIN_CATEGORY_DELETE)) {
            $this->commonServiceRegistry->getSwMainCategoryRepository()->delete(
                array_values($this->dataStore->getValues(self::DATA_STORE_KEY_PRODUCT_MAIN_CATEGORY_DELETE)),
                $this->commonServiceRegistry->getSwContext()
            );
        }

        // Delete outdated product category assignments.
        if (!empty($this->productCategoryDelete)) {
            $this->commonServiceRegistry->getSwProductCategoryRepository()->delete($this->productCategoryDelete, $this->getSwContext());
        }

        // Delete outdated product property assignments.
        if (!empty($this->productPropertyDelete)) {
            $this->commonServiceRegistry->getSwProductPropertyRepository()->delete($this->productPropertyDelete, $this->getSwContext());
        }

        // Delete outdated product property assignments.
        if (!empty($this->productOptionDelete)) {
            $this->commonServiceRegistry->getSwProductOptionRepository()->delete($this->productOptionDelete, $this->getSwContext());
        }

        if ($this->dataStore->hasValues(self::DATA_STORE_KEY_PRODUCT_TAG_DELETE)) {
            $this->commonServiceRegistry->getSwProductTagRepository()->delete(
                array_values($this->dataStore->getValues(self::DATA_STORE_KEY_PRODUCT_TAG_DELETE)),
                $this->getSwContext()
            );
        }

        // Delete existing configurator settings.
        $productConfiguratorDeletedProducts = [];
        if (!empty($this->productConfiguratorSettingsDelete)) {
            $this->commonServiceRegistry->getSwProductConfiguratorSettingRepository()->delete($this->productConfiguratorSettingsDelete, $this->getSwContext());
            foreach ($this->productConfiguratorSettingsDelete as $value) {
                $productConfiguratorDeletedProducts[] = $value['productId'];
            }
        }
        $productConfiguratorDeletedProducts = array_values(array_unique($productConfiguratorDeletedProducts));
        // Add configurator settings to product upsert data.
        foreach ($this->productConfiguratorSettingsCreate as $swId => $optionIds) {
            $optionIds = array_values(array_unique($optionIds));
            $existingConfiguratorSettingOptionIds = [];
            if (!array_key_exists($swId, $this->swEntityUpsertData)) {
                $this->swEntityUpsertData[$swId]['id'] = $swId;
            }

            if (
                !in_array($swId, $productConfiguratorDeletedProducts)
                && $this->additionalData['swExistingProducts']->has($swId)
            ) {
                // Only look in existing product if we did not deleted everything just in this step.
                /** @var ProductEntity $swProduct */
                $swProduct = $this->additionalData['swExistingProducts']->get($swId);

                foreach ($swProduct->getConfiguratorSettings() as $configuratorSetting) {
                    $existingConfiguratorSettingOptionIds[] = $configuratorSetting->getOptionId();
                }
            }

            foreach ($optionIds as $optionId) {
                if (!in_array($optionId, $existingConfiguratorSettingOptionIds)) {
                    // TODO: Add position.
                    $this->swEntityUpsertData[$swId]['configuratorSettings'][] = ['optionId' => $optionId];
                }
            }
        }
    }

    private function generateProductMediaRelationId(string $swpProductId, string $swpMediaId, int $position): string
    {
        return md5($swpProductId . '_' . $swpMediaId . '_' . $position);
    }

    private function getSwExistingProductByPlentyId(int $plentyId)
    {
        $swId = null;
        if (array_key_exists($plentyId, $this->plentyEntityId2SwId)) {
            $swId = $this->plentyEntityId2SwId[$plentyId];
        }

        /** @var null|Entity $swExistingEntity */
        $swExistingEntity = null;
        if ($this->additionalData['swExistingProducts']->has($swId)) {
            $swExistingEntity = $this->additionalData['swExistingProducts']->get($swId);
        }

        return $swExistingEntity;
    }

    public function getMappingEventName(): string
    {
        return 'lenz_platform_plenty_connector.mapping.product';
    }

    private function getCustomFieldTypeByPlentymarketsCastType(string $cast): string
    {
        if ($cast === 'float') {
            return CustomFieldTypes::FLOAT;
        } elseif ($cast === 'int') {
            return CustomFieldTypes::INT;
        } elseif ($cast === 'file') {
            return CustomFieldTypes::MEDIA;
        }

        // Fallback to html.
        return 'html';
    }

    private function getDebugPlentyUpdates(): EntityCollection
    {
        $response = $this->commonServiceRegistry->getRequestExecutor()->execute(
            $this->getDebugPlentyUpdateRequest($this->debugVariationId)
        );

        $aResponse = $response->toArray();

        if (!isset($aResponse['entries'][0])) {
            throw new \Exception('VariationID not found.');
        }

        $plentyUpdate = new ItemUpdateEntity();
        $plentyUpdate->setUniqueIdentifier('-');
        $plentyUpdate->setId('-');
        $plentyUpdate->setPlentyId($this->debugVariationId);
        $plentyUpdate->setVariationData($aResponse['entries'][0]);
        $plentyUpdate->setData([]);
        $plentyUpdate->setErrorCount(0);
        $plentyUpdates = new ItemUpdateCollection([$plentyUpdate]);

        return $plentyUpdates;
    }

    private function getDebugPlentyUpdateRequest(int $variationId): RequestInterface
    {
        /** @var GetVariationsRequest $request */
        $request = $this->commonServiceRegistry->getRequestBuilder(GetVariationsBaseRequestBuilder::NAME)->buildRequest(
            $this->commonServiceRegistry->getConfigService()->getPlentyUrl(),
            $this->commonServiceRegistry->getTokenHandler()->getAccessToken()
        );
        $request->setIds([$variationId]);

        return $request;
    }
}
