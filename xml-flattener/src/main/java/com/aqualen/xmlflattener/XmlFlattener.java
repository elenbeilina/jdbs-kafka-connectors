package com.aqualen.xmlflattener;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;

public class XmlFlattener<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final BiPredicate<Struct, Pair<String, String>> structType =
            (struct, type) -> struct.get(type.getKey()).equals(type.getValue());

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void configure(Map<String, ?> map) {
        //Transformation doesn't need to be configured
    }

    @Override
    public void close() {
        //Nothing to close
    }

    @Override
    public R apply(R r) {
        if (r.value() == null) {
            return r;
        }
        Schema asnSchema = SchemaBuilder.struct().name("Info")
                .field("ASNMessageID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("EstDepartDate", Date.SCHEMA)
                .field("EstDischargePortDate", Date.SCHEMA)
                .field("Mode", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ShipmentID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ShipmentStatus", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Vessel", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Voyage", Schema.OPTIONAL_STRING_SCHEMA)
                .field("OriginCityName", Schema.OPTIONAL_STRING_SCHEMA)
                .field("OriginCountryCode", Schema.OPTIONAL_STRING_SCHEMA)
                .field("PartyInfoName", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ContainerType", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ContainerLoad", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ContainerNumber", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ContainerTypeName", Schema.OPTIONAL_STRING_SCHEMA)
                .field("LoadPlanName", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ReferenceNumber", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SealNumber", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ServiceType", Schema.OPTIONAL_STRING_SCHEMA)
                .field("BLNumber", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Division", Schema.OPTIONAL_STRING_SCHEMA)
                .field("InvoiceNumber", Schema.OPTIONAL_STRING_SCHEMA)
                .field("LineItemNumber", Schema.OPTIONAL_STRING_SCHEMA)
                .field("LoadSequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ManufacturerID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ShippeCartoons", Schema.OPTIONAL_INT64_SCHEMA)
                .field("PONumber", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ProductCode", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ProductName", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ReferenceNumber1", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ReferenceNumber2", Schema.OPTIONAL_STRING_SCHEMA)
                .field("FactoryName", Schema.OPTIONAL_STRING_SCHEMA)
                .field("WeightUnit", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Weight", Decimal.schema(0))
                .field("TotalWeightUnit", Schema.OPTIONAL_STRING_SCHEMA)
                .field("TotalWeight", Decimal.schema(0))
                .field("SizeIndex", Schema.OPTIONAL_STRING_SCHEMA)
                .field("UPC", Schema.OPTIONAL_STRING_SCHEMA)
                .field("SKU", Schema.OPTIONAL_STRING_SCHEMA)
                .field("CartoonCount", Schema.OPTIONAL_INT64_SCHEMA)
                .field("MCCFlag_LegIndicator", Schema.OPTIONAL_STRING_SCHEMA)
                .field("OrderRef3", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Milestone-Transport_ATD_Origin", Date.builder().optional().schema())
                .field("Milestone-Transport_ETA_Origin", Date.builder().optional().schema())
                .field("Port_of_LoadingCountryCode", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Port_of_LoadingCity", Schema.OPTIONAL_STRING_SCHEMA)
                .field("PortofDischarge-CityName", Schema.OPTIONAL_STRING_SCHEMA)
                .field("PortofDischarge-CountryCode", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Shipped_Volume", Decimal.builder(0).optional().schema())
                .field("VolumeUnit", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Shipped_Qty", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("Shipped_Weight", Decimal.builder(0).optional().schema())
                .field("Shipped_Weight Unit", Schema.OPTIONAL_STRING_SCHEMA)
                .field("Transport_ETD_Origin", Date.builder().optional().schema())
                .build();

        Struct transformedValue = (Struct) r.value();
        Struct asn = extractStructFromArray(transformedValue, "ASN");

        Struct originCity = asn.getStruct("OriginCity");
        Struct partyInfo = extractStructFromArray(asn, "PartyInfo");

        Struct container = extractStructFromArray(asn, "Container");

        Struct lineItem = extractStructFromArray(container, "LineItems");
        Struct lineItemPartyInfo = extractStructFromArray(lineItem, "PartyInfo");
        Struct weight = lineItem.getStruct("Weight");

        Struct assortment = extractStructFromArray(lineItem, "Assortment");
        Struct totalWeight = assortment.getStruct("TotalWeight");

        Struct assortmentSize = extractStructFromArray(assortment, "References",
                new ImmutablePair<>("Type", "Size"));
        Struct lineItemBlRefNum1 = extractStructFromArray(lineItem, "References",
                new ImmutablePair<>("Type", "BlRefNum1"));
        Struct lineItemOrderRef3 = extractStructFromArray(lineItem, "References",
                new ImmutablePair<>("Type", "OrderRef3"));

        Struct referenceDateATDOrigin = extractStructFromArray(asn, "ReferenceDates",
                new ImmutablePair<>("ReferenceDateType", "Transport ATD Origin"));
        Struct referenceDateETADestination = extractStructFromArray(asn, "ReferenceDates",
                new ImmutablePair<>("ReferenceDateType", "Transport ETA Destination"));
        Struct referenceDateETDOrigin = extractStructFromArray(asn, "ReferenceDates",
                new ImmutablePair<>("ReferenceDateType", "Transport ETD Origin"));

        Struct portOfLoading = asn.getStruct("PortOfLoading");
        Struct portOfDischarge = asn.getStruct("PortOfDischarge");
        Struct volume = container.getStruct("Volume");
        Struct actualWight = container.getStruct("ActualWeight");

        Struct asnStruct = new Struct(asnSchema)
                .put("ASNMessageID", getFieldWIthNullCheck(asn, "ASNMessageID"))
                .put("EstDepartDate", asn.get("EstDepartDate"))
                .put("EstDischargePortDate", asn.get("EstDischargePortDate"))
                .put("Mode", asn.get("Mode"))
                .put("ShipmentID", asn.get("ShipmentID"))
                .put("ShipmentStatus", asn.get("ShipmentStatus"))
                .put("Vessel", asn.get("Vessel"))
                .put("Voyage", asn.get("Voyage"))
                .put("OriginCityName", originCity.get("CityName"))
                .put("OriginCountryCode", originCity.get("CountryCode"))
                .put("PartyInfoName", partyInfo.get("Name"))
                .put("ContainerType", container.get("ContainerType"))
                .put("ContainerLoad", container.get("ContainerLoad"))
                .put("ContainerNumber", container.get("ContainerNumber"))
                .put("ContainerTypeName", container.get("ContainerTypeName"))
                .put("LoadPlanName", container.get("LoadPlanName"))
                .put("ReferenceNumber", container.get("ReferenceNumber1"))
                .put("SealNumber", container.get("SealNumber"))
                .put("ServiceType", container.get("ServiceType"))
                .put("BLNumber", lineItem.get("BLNumber"))
                .put("Division", lineItem.get("DivisionIdentifier"))
                .put("InvoiceNumber", lineItem.get("InvoiceNumber"))
                .put("LineItemNumber", lineItem.get("LineItemNumber"))
                .put("LoadSequence", lineItem.get("LoadSequence"))
                .put("ManufacturerID", lineItem.get("ManufacturerID"))
                .put("ShippeCartoons", lineItem.get("PackageCount"))
                .put("PONumber", lineItem.get("PONumber"))
                .put("ProductCode", lineItem.get("ProductCode"))
                .put("ProductName", lineItem.get("ProductName"))
                .put("ReferenceNumber1", lineItem.get("ReferenceNumber1"))
                .put("ReferenceNumber2", lineItem.get("ReferenceNumber2"))
                .put("FactoryName", getFieldWIthNullCheck(lineItemPartyInfo, "Name"))
                .put("WeightUnit", weight.get("ANSICode"))
                .put("Weight", weight.get("value"))
                .put("TotalWeightUnit", totalWeight.get("ANSICode"))
                .put("TotalWeight", totalWeight.get("value"))
                .put("SizeIndex", getFieldWIthNullCheck(assortmentSize, "Value"))
                .put("UPC", assortment.get("UPC"))
                .put("SKU", assortment.get("Sku"))
                .put("CartoonCount", assortment.get("CartonCount"))
                .put("MCCFlag_LegIndicator", getFieldWIthNullCheck(lineItemBlRefNum1, "Value"))
                .put("OrderRef3", getFieldWIthNullCheck(lineItemOrderRef3, "Value"))
                .put("Milestone-Transport_ATD_Origin", getFieldWIthNullCheck(
                        referenceDateATDOrigin, "ReferenceDate")
                )
                .put("Milestone-Transport_ETA_Origin", getFieldWIthNullCheck(
                        referenceDateETADestination, "ReferenceDate")
                )
                .put("Port_of_LoadingCountryCode", portOfLoading.get("CountryCode"))
                .put("Port_of_LoadingCity", portOfLoading.get("CityName"))
                .put("PortofDischarge-CityName", portOfDischarge.get("CityName"))
                .put("PortofDischarge-CountryCode", portOfDischarge.get("CountryCode"))
                .put("Shipped_Volume", volume.get("value"))
                .put("VolumeUnit", volume.get("Unit"))
                .put("Shipped_Qty", getFieldWIthNullCheck(lineItem.getStruct("Quantity"), "value"))
                .put("Shipped_Weight", actualWight.get("value"))
                .put("Shipped_Weight Unit", actualWight.get("Unit"))
                .put("Transport_ETD_Origin", getFieldWIthNullCheck(referenceDateETDOrigin, "ReferenceDate"));
        try {
            return r.newRecord(
                    r.topic(),
                    r.kafkaPartition(),
                    r.keySchema(),
                    r.key(),
                    asnSchema,
                    asnStruct,
                    r.timestamp()
            );
        } catch (Exception e) {
            throw new MappingException(String.format("Unable to process message: %s", e.getMessage()), e);
        }
    }

    private Struct extractStructFromArray(Struct parent, String childName) {
        ArrayList<Struct> child = (ArrayList<Struct>) parent.get(childName);

        return Optional
                .ofNullable(child)
                .map(structs -> structs.get(0))
                .orElse(null);
    }

    private Struct extractStructFromArray(Struct parent, String childName, Pair<String, String> type) {
        ArrayList<Struct> child = (ArrayList<Struct>) parent.get(childName);

        Optional<Struct> structOptional = child
                .stream()
                .filter(struct -> structType.test(struct, type))
                .findFirst();
        return structOptional.orElse(null);
    }

    private Object getFieldWIthNullCheck(Struct struct, String fieldName) {
        return Optional.ofNullable(struct)
                .map(value -> value.get(fieldName))
                .orElse(null);
    }
}
