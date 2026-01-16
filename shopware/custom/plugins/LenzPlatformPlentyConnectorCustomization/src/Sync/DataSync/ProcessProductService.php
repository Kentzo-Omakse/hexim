<?php

namespace LenzPlatformPlentyConnectorCustomization\Sync\DataSync;

use LenzPlatformPlentyConnector\Core\Content\ItemUpdate\ItemUpdateCollection;
use LenzPlatformPlentyConnector\Core\Content\ItemUpdate\ItemUpdateEntity;
use LenzPlatformPlentyConnector\Service\CommonServiceRegistry;
use LenzPlatformPlentyConnector\Sync\DataSync\Mapping\MappingField;
use LenzPlatformPlentyConnector\Sync\DataSync\ProcessProductService as ParentProcessProductService;
use Shopware\Core\Content\ImportExport\Processing\Mapping\Mapping;
use Shopware\Core\Content\Product\ProductCollection;
use Shopware\Core\Framework\Context;
use Shopware\Core\Framework\DataAbstractionLayer\Entity;
use Shopware\Core\Framework\DataAbstractionLayer\EntityCollection;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Criteria;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Filter\EqualsAnyFilter;
use Shopware\Core\Framework\DataAbstractionLayer\Search\Filter\EqualsFilter;
use Shopware\Core\Framework\Uuid\Uuid;
use Shopware\Core\System\Unit\UnitCollection;

class ProcessProductService extends ParentProcessProductService
{
    private array $customizationUnitId2SwId = [];

    public function __construct(
        CommonServiceRegistry $commonServiceRegistry,
    ) {
        parent::__construct($commonServiceRegistry);
    }

    protected function preloadAdditionalData(EntityCollection $plentyEntityUpdates): void
    {
        parent::preloadAdditionalData($plentyEntityUpdates);

        $this->preloadAdditionalDataUnitsByProperty($plentyEntityUpdates);
    }

    protected function preloadAdditionalDataUnitsByProperty(EntityCollection $plentyEntityUpdates)
    {
        $this->customizationUnitId2SwId = [];
        /** @var ItemUpdateCollection $plentyEntityUpdates */

        $selectedValueIds = [];

        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (!array_key_exists('properties', $plentyEntityUpdate->getVariationData())) {
                continue;
            }

            foreach ($plentyEntityUpdate->getVariationData()['properties'] as $property) {
                if ($property['propertyId'] !== 45) {
                    continue;
                }

                $selectedValueIds[] = $property['selectionValues'][0]['selectionId'];
            }
        }

        $criteria = new Criteria();
        $criteria->addFilter(
            new EqualsAnyFilter('customFields.lenz_platform_plenty_connector_unit_property_selection_id', $selectedValueIds)
        );
        /** @var UnitCollection $units */
        $units = $this->commonServiceRegistry->getSwUnitRepository()->search($criteria, $this->commonServiceRegistry->getSwContext())->getEntities();
        $unitIdByPropertySelectionId = [];
        foreach ($units as $unit) {
            $unitIdByPropertySelectionId[(int) $unit->getTranslation('customFields')['lenz_platform_plenty_connector_unit_property_selection_id']] = $unit->getId();
            $this->customizationUnitId2SwId[(int) $unit->getTranslation('customFields')['lenz_platform_plenty_connector_unit_property_selection_id']] = $unit->getId();
        }

        $unitsToCreate = [];

        foreach ($plentyEntityUpdates as $plentyEntityUpdate) {
            if (!array_key_exists('properties', $plentyEntityUpdate->getVariationData())) {
                continue;
            }

            foreach ($plentyEntityUpdate->getVariationData()['properties'] as $property) {
                if ($property['propertyId'] !== 45) {
                    continue;
                }

                $selectedValue = $property['selectionValues'][0]['selectionId'];

                if (array_key_exists($selectedValue, $unitIdByPropertySelectionId)) {
                    continue;
                }

                if (array_key_exists($selectedValue, $this->customizationUnitId2SwId)) {
                    continue;
                }

                foreach ($property['property']['selections'] as $selection) {
                    if ($selection['id'] !== $selectedValue) {
                        continue;
                    }

                    $unitToCreate = [
                        'id' => Uuid::randomHex(),
                        'shortCode' => '-',
                    ];

                    foreach ($this->swLanguages as $language) {
                        $unitToCreate['translations'][$language->getId()]['customFields']['lenz_platform_plenty_connector_unit_property_selection_id'] = $selectedValue;
                        $unitToCreate['translations'][$language->getId()]['name'] = $this->commonServiceRegistry->getMappingValueConverter()->convert($selection, 'names.0.name', $language);
                    }

                    $unitsToCreate[] = $unitToCreate;

                    $this->customizationUnitId2SwId[$selectedValue] = $unitToCreate['id'];
                }
            }
        }

        if (!empty($unitsToCreate)) {
            $this->commonServiceRegistry->getSwUnitRepository()->create(array_values($unitsToCreate), $this->commonServiceRegistry->getSwContext());
        }
    }

    public function getMapping(): array
    {
        $mapping = parent::getMapping();

        // Do your modifications here.
        // Modify existing values.
        /**
         * @var int $key
         * @var MappingField $value
         */
        foreach ($mapping as $key => $value) {
            if ($value->getSwFieldName() === 'name') {
                // Remove.
//                unset($mapping[$key]);
                // Change definition.
//                $mapping[$key] = new MappingField('name', MappingField::TYPE_STRING, ['item.texts.0.name2']); // sw, typ, field
            }


            // variant id as productNumber
            if ($value->getSwFieldName() === 'productNumber') {
                unset($mapping[$key]);
                $mapping[$key] = (new MappingField('productNumber', MappingField::TYPE_STRING, ['variation.id']));
            }

            // productNumber as manufacturer id
            if ($value->getSwFieldName() === 'manufacturerNumber') {
                unset($mapping[$key]);
                $mapping[$key] = new MappingField('manufacturerNumber', MappingField::TYPE_STRING, ['variation.base.number']);
            }

            // meta title
            if ($value->getSwFieldName() === 'metaTitle') {
                unset($mapping[$key]);
                $mapping[$key] = new MappingField('metaTitle', MappingField::TYPE_STRING, ['variation.base.texts.0.name3']);
            }

            // meta keywords
            if ($value->getSwFieldName() === 'keywords') {
                unset($mapping[$key]);
                $mapping[$key] = new MappingField('keywords', MappingField::TYPE_STRING, ['variation.base.texts.0.metaKeywords']);
            }

            // attributes
            // menge
            if ($value->getSwFieldName() === 'purchaseUnit') {
                unset($mapping[$key]);
                $mapping[] = new MappingField('purchaseUnit', MappingField::TYPE_STRING, ['_dynamicProperties.14.value']);
            }

            // TODO
            // verkaufseinheit name: id 45
            if ($value->getSwFieldName() === 'unitId') {
                unset($mapping[$key]);
                $mapping[] = (new MappingField('unitId', MappingField::TYPE_STRING, ['_dynamicProperties.45'], function ($value) {
                    $plentyPropertyOptionId = $value['selectionValues'][0]['selectionId'];

                    if (array_key_exists($plentyPropertyOptionId, $this->customizationUnitId2SwId)) {
                        return $this->customizationUnitId2SwId[$plentyPropertyOptionId];
                    }

                    return null;
                }));
            }
        }

        // plenty custom field
        $mapping[] = new MappingField('customFields.product_item_id', MappingField::TYPE_STRING, ['variation.base.itemId']);

        // youtube url
        $mapping[] = (new MappingField('customFields.zenit_gravity_youtube_ids', MappingField::TYPE_STRING, ['_dynamicProperties.288.values.0.value']))
            ->setTranslatedField(true);



        // TODO
        // bullet points
        // property ids 29 - 33, concatenated with ';'
        // check for language!
        // write these via free text?!
        // $mapping[] = (new MappingField('customFields.zenit_gravity_features', MappingField::TYPE_STRING, ['_dynamicProperties'], function ($value, $accessPath, $language) {
        //     $aNewValues = [];

        //     $aNewValues[] = $this->commonServiceRegistry->getMappingValueConverter()->convert($value, '29.values.0.value', $language);
        //     $aNewValues[] = $this->commonServiceRegistry->getMappingValueConverter()->convert($value, '30.values.0.value', $language);
        //     $aNewValues[] = $this->commonServiceRegistry->getMappingValueConverter()->convert($value, '31.values.0.value', $language);
        //     $aNewValues[] = $this->commonServiceRegistry->getMappingValueConverter()->convert($value, '32.values.0.value', $language);
        //     $aNewValues[] = $this->commonServiceRegistry->getMappingValueConverter()->convert($value, '33.values.0.value', $language);

        //     foreach ($aNewValues as $aNewValueKey => $aNewValue) {
        //         if (empty($aNewValue)) {
        //             unset($aNewValues[$aNewValueKey]);
        //         }
        //     }

        //     return join(';', $aNewValues);
        // }))
        //     ->setTranslatedField(true)
        // ;


        // TODO
        // search keywords
        $mapping[] = (new MappingField('customSearchKeywords', MappingField::TYPE_ARRAY, ['variation.base.texts.0.metaKeywords'], function ($value, $accessPath) {
            return explode(', ', $value);
        }))
            ->setTranslatedField(true)
        ;


        //$mapping[] = (new MappingField('width', MappingField::TYPE_FLOAT, ['variation.base.widthMM']));

        //        $mapping[] = new MappingField('customFields.lenz_platform_plenty_connector_product_test', ['variation.isInvisibleInListIfNetStockIsNotPositive', '']);
        //        $mapping[] = (new MappingField('customFields.test', MappingField::TYPE_ARRAY, ['variation.categories.0.category.details.0.name']))
        //            ->setTranslatedField(true);

        // TODO
        // eigenschaften bilder at 'https://www.hexim.de/images/produkte/grp/
        // will this url change in production?

        return $mapping;
    }

    protected function manipulateConvertedEntity(array $convertedEntity, Entity $plentyEntityUpdate): ?array
    {
        /** @var ItemUpdateEntity $plentyEntityUpdate */
        $convertedEntity = parent::manipulateConvertedEntity($convertedEntity, $plentyEntityUpdate);
        // Do your modifications here.

        return $convertedEntity;
    }
}
