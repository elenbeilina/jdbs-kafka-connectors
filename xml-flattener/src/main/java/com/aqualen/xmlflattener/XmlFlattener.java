package com.aqualen.xmlflattener;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.logging.Logger;

public class XmlFlattener<R extends ConnectRecord<R>> implements Transformation<R> {

    private static Logger log = Logger.getLogger(XmlFlattener.class.getName());
    Predicate<Struct> sizeType = struct -> struct.get("Type").equals("Size");

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
        Schema asnSchema = SchemaBuilder.struct().name("ASN")
                .field("ASNMessageID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("EstDepartDate", Date.SCHEMA)
                .field("EstDischargePortDate", Date.SCHEMA)
                .field("Mode", Schema.STRING_SCHEMA)
                .field("ShipmentID", Schema.STRING_SCHEMA)
                .field("ShipmentStatus", Schema.STRING_SCHEMA)
                .field("Vessel", Schema.STRING_SCHEMA)
                .field("Voyage", Schema.STRING_SCHEMA)
                .field("OriginCityName", Schema.STRING_SCHEMA)
                .field("OriginCountryCode", Schema.STRING_SCHEMA)
                .field("PartyInfoName", Schema.STRING_SCHEMA)
                .field("ContainerType", Schema.STRING_SCHEMA)
                .field("ContainerLoad", Schema.STRING_SCHEMA)
                .field("ContainerNumber", Schema.STRING_SCHEMA)
                .field("ContainerTypeName", Schema.STRING_SCHEMA)
                .field("LoadPlanName", Schema.STRING_SCHEMA)
                .field("ReferenceNumber", Schema.STRING_SCHEMA)
                .field("SealNumber", Schema.STRING_SCHEMA)
                .field("ServiceType", Schema.STRING_SCHEMA)
                .field("BLNumber", Schema.STRING_SCHEMA)
                .field("Division", Schema.STRING_SCHEMA)
                .field("InvoiceNumber", Schema.STRING_SCHEMA)
                .field("LineItemNumber", Schema.STRING_SCHEMA)
                .field("LoadSequence", Schema.STRING_SCHEMA)
                .field("ManufacturerID", Schema.STRING_SCHEMA)
                .field("ShippeCartoons", Schema.STRING_SCHEMA)
                .field("PONumber", Schema.STRING_SCHEMA)
                .field("ProductCode", Schema.STRING_SCHEMA)
                .field("ProductName", Schema.STRING_SCHEMA)
                .field("ReferenceNumber1", Schema.STRING_SCHEMA)
                .field("ReferenceNumber2", Schema.STRING_SCHEMA)
                .field("FactoryName", Schema.STRING_SCHEMA)
                .field("WeightUnit", Schema.STRING_SCHEMA)
                .field("Weight", Schema.INT16_SCHEMA)
                .field("TotalWeightUnit", Schema.STRING_SCHEMA)
                .field("TotalWeight", Schema.INT16_SCHEMA)
                .field("SizeIndex", Schema.STRING_SCHEMA)
                .field("UPC", Schema.STRING_SCHEMA)
                .field("SKU", Schema.STRING_SCHEMA)
                .field("CartoonCount", Schema.STRING_SCHEMA)
                .field("MCCFlag_LegIndicator", Schema.STRING_SCHEMA)
                .field("OrderRef3", Schema.STRING_SCHEMA)
                .field("Milestone - Actual time of arrival (ATA)", Schema.STRING_SCHEMA)
                .field("Milestone - Transport ATD Origin", Schema.STRING_SCHEMA)
                .field("Milestone - Transport ETA Origin", Schema.STRING_SCHEMA)
                .field("Gate in Origin", Schema.STRING_SCHEMA)
                .field("Port of LoadingCountryCode", Schema.STRING_SCHEMA)
                .field("Port of LoadingCity", Schema.STRING_SCHEMA)
                .field("PortofDischarge-CityName", Schema.STRING_SCHEMA)
                .field("PortofDischarge-CountryCode", Schema.STRING_SCHEMA)
                .field("Received (Pickup, CFS or CY) - FCR date", Schema.STRING_SCHEMA)
                .field("Shipped Volume", Schema.STRING_SCHEMA)
                .field("VolumeUnit", Schema.STRING_SCHEMA)
                .field("Shipped Qty", Schema.STRING_SCHEMA)
                .field("Shipped Weight", Schema.STRING_SCHEMA)
                .field("Shipped Weight Unit", Schema.STRING_SCHEMA)
                .field("Transport ETD Origin", Schema.STRING_SCHEMA)
                .build();

        Struct transformedValue = (Struct) r.value();
        ArrayList<Struct> asnList = (ArrayList<Struct>) transformedValue.get("ASN");
        Struct asn = asnList.get(0) ;

        Struct originCity = asn.getStruct("OriginCity");
        Struct asnPartyInfo = asn.getStruct("PartyInfo");
        Struct container = asn.getStruct("Container");

        ArrayList<Struct> lineItems = (ArrayList<Struct>)container.get("LineItems");
        Struct lineItem = lineItems.get(0);
        Struct lineItemPartyInfo = lineItem.getStruct("PartyInfo");
        Struct weight = lineItem.getStruct("Weight");

        Struct assortment = lineItem.getStruct("Assortment");
        Struct totalWeight = assortment.getStruct("TotalWeight");
        ArrayList<Struct> references = (ArrayList<Struct>) assortment.get("References");
        Struct sizeIndex = references.stream().filter(sizeType).findFirst().get();


        Struct asnStruct = new Struct(asnSchema)
                .put("ASNMessageID", Optional.ofNullable(asn.get("ASNMessageID")).orElse(null))
                .put("EstDepartDate", asn.get("EstDepartDate"))
                .put("EstDischargePortDate", asn.get("EstDischargePortDate"))
                .put("Mode", asn.get("Mode"))
                .put("ShipmentID", asn.get("ShipmentID"))
                .put("ShipmentStatus", asn.get("ShipmentStatus"))
                .put("Vessel", asn.get("Vessel"))
                .put("Voyage", asn.get("Voyage"))
                .put("OriginCityName", originCity.get("CityName"))
                .put("OriginCountryCode", originCity.get("CountryCode"))
                .put("PartyInfoName", asnPartyInfo.get("Name"))
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
                .put("FactoryName", lineItemPartyInfo.get("Name"))
                .put("WeightUnit", weight.get("ANSICode"))
                .put("Weight", weight.get("value"))
                .put("TotalWeightUnit", totalWeight.get("ANSICode"))
                .put("TotalWeight", totalWeight.get("value"))
                .put("SizeIndex", sizeIndex.get("Value"))
                .put("UPC", assortment.get("UPC"))
                .put("SKU", assortment.get("SKU"))
                .put("CartoonCount", assortment.get("CartoonCount"))
//                .put("MCCFlag_LegIndicator", asn.get("EstDischargePortDate"))
//                .put("OrderRef3", asn.get("EstDischargePortDate"))
//                .put("Milestone - Actual time of arrival (ATA)", asn.get("EstDischargePortDate"))
//                .put("Milestone - Transport ATD Origin", asn.get("EstDischargePortDate"))
//                .put("Milestone - Transport ETA Origin", asn.get("EstDischargePortDate"))
//                .put("Gate in Origin", asn.get("EstDischargePortDate"))
//                .put("Port of LoadingCountryCode", asn.get("EstDischargePortDate"))
//                .put("Port of LoadingCity", asn.get("EstDischargePortDate"))
//                .put("PortofDischarge-CityName", asn.get("EstDischargePortDate"))
//                .put("PortofDischarge-CountryCode", asn.get("EstDischargePortDate"))
//                .put("Received (Pickup, CFS or CY) - FCR date", asn.get("EstDischargePortDate"))
//                .put("Shipped Volume", asn.get("EstDischargePortDate"))
//                .put("VolumeUnit", asn.get("EstDischargePortDate"))
//                .put("Shipped Qty", asn.get("EstDischargePortDate"))
//                .put("Shipped Weight", asn.get("EstDischargePortDate"))
//                .put("Shipped Weight Unit", asn.get("EstDischargePortDate"))
//                .put("Transport ETD Origin", asn.get("EstDischargePortDate"))
        ;
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
}
