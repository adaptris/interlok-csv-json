package com.adaptris.core.transform.csvjson;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceCase;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.constraints.AssertTrue;
import java.io.IOException;

public class JsonToFixedCSVTest extends ServiceCase
{
	private static final String CSV_HEADER = "GrowerId,FarmID,SyncID,ID,ModifiedOn,CreatedOn,AdminID,CreatorID,EditorID,FieldID,TotalFieldArea,TillageStyle,PrimaryImplement,SecondaryImplement,TillageDepth,TillageUnit,ImplementWidth,ImplementUnit,TillageDirection,CropID,SeedCompanyID,VarietyHybridID,SeedingRate,SeedingRateUnit,CropPurposeID,LotNumber,PlantingDepth,PlantingDepthUnit,SeedTreatmentID0,SeedTreatmentRate0,SeedTreatmentUnit0,SeedTreatmentRegistrationCode0,SeedTreatmentBatchNumber0,SeedTreatmentID1,SeedTreatmentRate1,SeedTreatmentUnit1,SeedTreatmentRegistrationCode1,SeedTreatmentBatchNumber1,SeedTreatmentID2,SeedTreatmentRate2,SeedTreatmentUnit2,SeedTreatmentRegistrationCode2,SeedTreatmentBatchNumber2,SeedTreatmentID3,SeedTreatmentRate3,SeedTreatmentUnit3,SeedTreatmentRegistrationCode3,SeedTreatmentBatchNumber3,PlanterCompanyID,PlanterClass,PlanterModelID,ReplantID,DoubleCropID,BinRunSeedID,CommercialFertilizerID0,CommercialFertilizerRate0,CommercialFertilizerUnit0,CommercialFertilizerBatchNumber0,CommercialFertilizerMixOrder0,CommercialFertilizerID1,CommercialFertilizerRate1,CommercialFertilizerUnit1,CommercialFertilizerBatchNumber1,CommercialFertilizerMixOrder1,CommercialFertilizerID2,CommercialFertilizerRate2,CommercialFertilizerUnit2,CommercialFertilizerBatchNumber2,CommercialFertilizerMixOrder2,CommercialFertilizerID3,CommercialFertilizerRate3,CommercialFertilizerUnit3,CommercialFertilizerBatchNumber3,CommercialFertilizerMixOrder3,CommercialFertilizerID4,CommercialFertilizerRate4,CommercialFertilizerUnit4,CommercialFertilizerBatchNumber4,CommercialFertilizerMixOrder4,CommercialFertilizerID5,CommercialFertilizerRate5,CommercialFertilizerUnit5,CommercialFertilizerBatchNumber5,CommercialFertilizerMixOrder5,CustomBlendFertilizerID0,CustomBlendFertilizerMeasure0,CustomBlendFertilizerUnit0,CustomBlendFertilizerBatchNumber0,CustomBlendFertilizerMixOrder0,CustomBlendFertilizerID1,CustomBlendFertilizerMeasure1,CustomBlendFertilizerUnit1,CustomBlendFertilizerBatchNumber1,CustomBlendFertilizerMixOrder1,CustomBlendFertilizerID2,CustomBlendFertilizerMeasure2,CustomBlendFertilizerUnit2,CustomBlendFertilizerBatchNumber2,CustomBlendFertilizerMixOrder2,CustomBlendFertilizerID3,CustomBlendFertilizerMeasure3,CustomBlendFertilizerUnit3,CustomBlendFertilizerBatchNumber3,CustomBlendFertilizerMixOrder3,CustomBlendFertilizerID4,CustomBlendFertilizerMeasure4,CustomBlendFertilizerUnit4,CustomBlendFertilizerBatchNumber4,CustomBlendFertilizerMixOrder4,CustomBlendFertilizerID5,CustomBlendFertilizerMeasure5,CustomBlendFertilizerUnit5,CustomBlendFertilizerBatchNumber5,CustomBlendFertilizerMixOrder5,ProductSource,ProductSourceContactPerson,HerbicideID0,HerbicideRate0,HerbicideRateUnit0,HerbicideRegistrationCodeID0,HerbicideRegistrationBatchNumber0,HerbicideRegistrationMixOrder0,HerbicideID1,HerbicideRate1,HerbicideRateUnit1,HerbicideRegistrationCodeID1,HerbicideRegistrationBatchNumber1,HerbicideRegistrationMixOrder1,HerbicideID2,HerbicideRate2,HerbicideRateUnit2,HerbicideRegistrationCodeID2,HerbicideRegistrationBatchNumber2,HerbicideRegistrationMixOrder2,HerbicideID3,HerbicideRate3,HerbicideRateUnit3,HerbicideRegistrationCodeID3,HerbicideRegistrationBatchNumber3,HerbicideRegistrationMixOrder3,HerbicideID4,HerbicideRate4,HerbicideRateUnit4,HerbicideRegistrationCodeID4,HerbicideRegistrationBatchNumber4,HerbicideRegistrationMixOrder4,HerbicideID5,HerbicideRate5,HerbicideRateUnit5,HerbicideRegistrationCodeID5,HerbicideRegistrationBatchNumber5,HerbicideRegistrationMixOrder5,HerbicideID6,HerbicideRate6,HerbicideRateUnit6,HerbicideRegistrationCodeID6,HerbicideRegistrationBatchNumber6,HerbicideRegistrationMixOrder6,HerbicideID7,HerbicideRate7,HerbicideRateUnit7,HerbicideRegistrationCodeID7,HerbicideRegistrationBatchNumber7,HerbicideRegistrationMixOrder7,PrimaryTargetWeedID0,PrimaryTargetWeedID1,PrimaryTargetWeedID2,SecondaryTargetWeedID0,SecondaryTargetWeedID1,SecondaryTargetWeedID2,InsecticideID0,InsecticideRate0,InsecticideUnit0,InsecticideRegistrationCode0,InsecticideRegistrationBatchNumber0,InsecticideRegistrationMixOrder0,InsecticideID1,InsecticideRate1,InsecticideUnit1,InsecticideRegistrationCode1,InsecticideRegistrationBatchNumber1,InsecticideRegistrationMixOrder1,InsecticideID2,InsecticideRate2,InsecticideUnit2,InsecticideRegistrationCode2,InsecticideRegistrationBatchNumber2,InsecticideRegistrationMixOrder2,InsecticideID3,InsecticideRate3,InsecticideUnit3,InsecticideRegistrationCode3,InsecticideRegistrationBatchNumber3,InsecticideRegistrationMixOrder3,InsecticideID4,InsecticideRate4,InsecticideUnit4,InsecticideRegistrationCode4,InsecticideRegistrationBatchNumber4,InsecticideRegistrationMixOrder4,InsecticideID5,InsecticideRate5,InsecticideUnit5,InsecticideRegistrationCode5,InsecticideRegistrationBatchNumber5,InsecticideRegistrationMixOrder5,InsecticideID6,InsecticideRate6,InsecticideUnit6,InsecticideRegistrationCode6,InsecticideRegistrationBatchNumber6,InsecticideRegistrationMixOrder6,InsecticideID7,InsecticideRate7,InsecticideUnit7,InsecticideRegistrationCode7,InsecticideRegistrationBatchNumber7,InsecticideRegistrationMixOrder7,PrimaryTargetInsectID0,PrimaryTargetInsectID1,PrimaryTargetInsectID2,SecondaryTargetInsectID0,SecondaryTargetInsectID1,SecondaryTargetInsectID2,FungicideID0,FungicideRate0,FungicideRateUnit0,FungicideRegistrationCodeID0,FungicideRegistrationBatchNumber0,FungicideRegistrationMixOrder0,FungicideID1,FungicideRate1,FungicideRateUnit1,FungicideRegistrationCodeID1,FungicideRegistrationBatchNumber1,FungicideRegistrationMixOrder1,FungicideID2,FungicideRate2,FungicideRateUnit2,FungicideRegistrationCodeID2,FungicideRegistrationBatchNumber2,FungicideRegistrationMixOrder2,FungicideID3,FungicideRate3,FungicideRateUnit3,FungicideRegistrationCodeID3,FungicideRegistrationBatchNumber3,FungicideRegistrationMixOrder3,FungicideID4,FungicideRate4,FungicideRateUnit4,FungicideRegistrationCodeID4,FungicideRegistrationBatchNumber4,FungicideRegistrationMixOrder4,FungicideID5,FungicideRate5,FungicideRateUnit5,FungicideRegistrationCodeID5,FungicideRegistrationBatchNumber5,FungicideRegistrationMixOrder5,FungicideID6,FungicideRate6,FungicideRateUnit6,FungicideRegistrationCodeID6,FungicideRegistrationBatchNumber6,FungicideRegistrationMixOrder6,FungicideID7,FungicideRate7,FungicideRateUnit7,FungicideRegistrationCodeID7,FungicideRegistrationBatchNumber7,FungicideRegistrationMixOrder7,PrimaryTargetDiseaseID0,PrimaryTargetDiseaseID1,PrimaryTargetDiseaseID2,SecondaryTargetDiseaseID0,SecondaryTargetDiseaseID1,SecondaryTargetDiseaseID2,GrowthRegulatorID0,GrowthRegulatorRate0,GrowthRegulatorRateUnit0,GrowthRegulatorRegistrationCode0,GrowthRegulatorBatchNumber0,GrowthRegulatorMixOrder0,GrowthRegulatorID1,GrowthRegulatorRate1,GrowthRegulatorRateUnit1,GrowthRegulatorRegistrationCode1,GrowthRegulatorBatchNumber1,GrowthRegulatorMixOrder1,GrowthRegulatorID2,GrowthRegulatorRate2,GrowthRegulatorRateUnit2,GrowthRegulatorRegistrationCode2,GrowthRegulatorBatchNumber2,GrowthRegulatorMixOrder2,GrowthRegulatorID3,GrowthRegulatorRate3,GrowthRegulatorRateUnit3,GrowthRegulatorRegistrationCode3,GrowthRegulatorBatchNumber3,GrowthRegulatorMixOrder3,ApplicationMethodID,ApplicationTimingID,PlantSpacing,PlantSpacingUnit,RowSpacing,RowSpacingUnit,RowOrientationID,AdditiveID0,AdditiveRate0,AdditiveRateUnit0,AdditiveBatchNumber0,AdditiveMixOrder0,AdditiveID1,AdditiveRate1,AdditiveRateUnit1,AdditiveBatchNumber1,AdditiveMixOrder1,AdditiveID2,AdditiveRate2,AdditiveRateUnit2,AdditiveBatchNumber2,AdditiveMixOrder2,AdditiveID3,AdditiveRate3,AdditiveRateUnit3,AdditiveBatchNumber3,AdditiveMixOrder3,AdditiveID4,AdditiveRate4,AdditiveRateUnit4,AdditiveBatchNumber4,AdditiveMixOrder4,AdditiveID5,AdditiveRate5,AdditiveRateUnit5,AdditiveBatchNumber5,AdditiveMixOrder5,NitrogenStabilizerID,NitrogenStabilizerRate,NitrogenStabilizerRateUnit,NitrogenStabilizerRegistrationCodeID,NitrogenStabilizerBatchNumber,NitrogenStabilizerMixOrder,TotalApplicationCarrierID,TotalApplicationMixOrder,TotalApplicationRate,TotalApplicationRateUnit,REI,REIUnit,PHI_WHP,PHI_WHPUnit,PlantBack,PlantBackUnit,BandWidth,BandWidthUnit,BandingPercentage,SoilConditionID,SoilTemperature,SoilTemperatureUnit,TimeUntilIncorporation,TimeUntilIncorporationUnit,YieldGoalUnit,PercentCropResidue,ClosestSurfaceWaterDistance,ClosestSurfaceWaterDistanceUnit,SurfaceWaterName,SetBackAdheredID,PotentialSensitiveAreaID,BufferStripTypeID,OverallWeatherID,WindDirectionID,WindSpeed,WindSpeedUnit,AirTemperature,AirTemperatureInCanopy,AirTemperatureUnit,PercentRelativeHumidity,PercentRelativeHumidityInCanopy,DeltaT,DeltaTUnit,PreAppRainfallAmountLast24Hr,PreAppRainfallAmountLast24HrUnit,PostAppRainfallAmountLast24Hr,PostAppRainfallAmountLast24HrUnit,TileInspection,HourlyOutletInspectionID,StartTime,StartAMorPMID,EndTime,EndAMorPMID,AverageSpeed,AverageSpeedUnit,TractorCompanyID,TractorModelID,TractorName,HoursOfTractorOperation,ApplicationEquipmentCompanyID,ApplicationEquipmentClassID,ApplicationEquipmentModelID,NozzleCompany,NozzleModel,NozzleSpacing,NozzleSpacingUnit,DropletSize,NozzlePressure,NozzlePressureUnit,ApplicatorTypeID,EquipmentName,ProductReleaseHeight,ProductReleaseHeightUnit,CalibrationFrequency,CalibrationFrequencyUnit,TotalNumberOfLoads,AverageLoadSize,AverageLoadSizeUnit,TractorWithCab,ApronWorn,BootsWorn,GlovesWorn,GogglesWorn,HeadProtectionWorn,LabelRead,NeighborsNotified,OverallsWorn,RespiratorWorn,RiskAssessmentUndertaken,SDSRead,CustomOperation,CustomOperationFirmName,UsedGPSID,Operator,OperatorLicenseNumber,Comments";

	private static final String JSON_ARRAY_RESOURCE = "json-csv-array.json";
	private static final String CSV_ARRAY_NO_HEADER = "43edf03f-a0b7-4679-b2cd-6597c1241cfc,5fc63839-d4ce-4f34-8fe6-f58640ebca09,13,5559354a-0b5d-4cdc-9cd0-a985f446c62d,2017-09-08T16:20:04Z,2016-12-19T18:36:21Z,13,13,13,b788b9c4-1fe6-4007-9889-e6445ee3727b,85.14494974216365,7,6,152,6.0,Tillage Depth Units,16.0,Implement Width Units,5,11,712,64821,32000.0,Seeding Units,5,123456,6.0,Planting Depth Units,9240,5.0,Seed Treatment Units,9651,1,10549,4.0,Seed Treatment Units,11174,2,9412,5.0,Seed Treatment Units,9864,3,9587,4.5,Seed Treatment Units,10086,4,354,7,705,1,2,2,13,45.0,Fertilizer Rate Units,1,1.0,20,45.0,Fertilizer Rate Units,2,2.0,1581,41.0,Fertilizer Rate Units,3,3.0,977,13.0,Fertilizer Rate Units,4,4.0,1701,45.0,Fertilizer Rate Units,5,5.0,936,10.0,Fertilizer Rate Units,6,6.0,59f19149-08e8-4547-be1f-3e0bf80f75cf,25.0,Custom Fertilizer Units,1,1.0,0ae23dce-a6f9-4729-8097-dc48f9253773,12.0,Custom Fertilizer Units,2,2.0,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,30.0,Custom Fertilizer Units,3,3.0,3d95ceba-e129-43da-a7a7-dc31b119a166,10.0,Custom Fertilizer Units,4,4.0,61ef6c2e-4b10-4cad-b351-a4fff5afa801,21.0,Custom Fertilizer Units,5,5.0,181139ce-59de-4037-9367-bd3cfe7ea74f,10.0,Custom Fertilizer Units,6,6.0,Helena,John,4692,4.0,Herbicide Units,11394,1,1.0,11582,6.0,Herbicide Units,12558,2,2.0,10528,6.0,Herbicide Units,11150,3,3.0,11379,4.0,Herbicide Units,12273,4,4.0,11502,3.0,Herbicide Units,12457,5,5.0,9329,6.0,Herbicide Units,9751,6,6.0,11228,11.0,Herbicide Units,12039,7,7.0,591,11.0,Herbicide Units,591,8,8.0,310,349,349,1060,1060,1060,9359,4.0,Insecticide Units,9789,1,1.0,10003,3.0,Insecticide Units,10542,2,2.0,9571,6.0,Insecticide Units,10070,3,3.0,7496,4.0,Insecticide Units,7496,4,4.0,7642,3.0,Insecticide Units,7642,5,5.0,8390,4.0,Insecticide Units,8478,6,6.0,7589,6.0,Insecticide Units,8740,7,7.0,9562,6.0,Insecticide Units,10061,8,8.0,390,199,583,362,61,499,10300,4.0,Fungicide Units,10887,1,1.0,11388,7.0,Fungicide Units,12282,2,2.0,5444,4.0,Fungicide Units,5444,3,3.0,9195,4.0,Fungicide Units,9591,4,4.0,5266,3.0,Fungicide Units,5266,5,5.0,5345,4.0,Fungicide Units,5345,6,6.0,5423,4.0,Fungicide Units,5423,7,7.0,11581,2.0,Fungicide Units,12557,8,8.0,277,447,163,277,366,180,9164,4.0,Growth Regulator Units,9552,1,1.0,6023,2.0,Growth Regulator Units,10005,2,2.0,6058,4.5,Growth Regulator Units,6058,3,3.0,9176,4.0,Growth Regulator Units,9565,5,4.0,28,1,12.0,Plant Spacing Units,12.0,Row Spacing Units,5,5,4.0,Additive Units,1,1.0,120,6.0,Additive Units,2,2.0,892,4.0,Additive Units,3,3.0,510,6.0,Additive Units,4,4.0,730,4.5,Additive Units,5,5.0,731,6.0,Additive Units,6,6.0,13,1.0,Nitrogen Stabilizer Units,13,1,1.0,5,1.0,120.0,Total App. Units,4.0,REI Units,5.0,PHI/WHP Units,3.0,Plant Back Units,5.0,Band Width Units,10.0,4,68.0,Soil Temperature Units,1,Time Units,Yield Goal Units,40,5.0,Distance Units,lake,2,7,8,11,2,9.0,Wind Speed Units,68.0,75.0,Air Temperature Units,45,90.0,4.0,Delta T Units,0.01,Pre-App Rainfall Units,0.2,Post-App Rainfall Units,1,2,18,1,17,1,8.0,Speed Units,394,511,red,250.0,1158,1,280,1426,56,18.0,Nozzle Spacing Units,2,45.0,Spray Pressure Units,3,sprayer,24.0,Height Units,1,Frequency Units,4.0,400,Load Size Units,1,2,2,2,1,1,1,2,1,1,1,1,2,spraying co.,1,Austin,123456,here are some comments\r\n" +
			"43edf03f-a0b7-4679-b2cd-6597c1241cfc,5fc63839-d4ce-4f34-8fe6-f58640ebca09,13,5559354a-0b5d-4cdc-9cd0-a985f446c62d,2017-09-08T16:20:04Z,2016-12-19T18:36:21Z,13,13,13,b788b9c4-1fe6-4007-9889-e6445ee3727b,85.14494974216365,7,6,152,6.0,Tillage Depth Units,16.0,Implement Width Units,5,11,712,64821,32000.0,Seeding Units,5,123456,6.0,Planting Depth Units,9240,5.0,Seed Treatment Units,9651,1,10549,4.0,Seed Treatment Units,11174,2,9412,5.0,Seed Treatment Units,9864,3,9587,4.5,Seed Treatment Units,10086,4,354,7,705,1,2,2,13,45.0,Fertilizer Rate Units,1,1.0,20,45.0,Fertilizer Rate Units,2,2.0,1581,41.0,Fertilizer Rate Units,3,3.0,977,13.0,Fertilizer Rate Units,4,4.0,1701,45.0,Fertilizer Rate Units,5,5.0,936,10.0,Fertilizer Rate Units,6,6.0,59f19149-08e8-4547-be1f-3e0bf80f75cf,25.0,Custom Fertilizer Units,1,1.0,0ae23dce-a6f9-4729-8097-dc48f9253773,12.0,Custom Fertilizer Units,2,2.0,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,30.0,Custom Fertilizer Units,3,3.0,3d95ceba-e129-43da-a7a7-dc31b119a166,10.0,Custom Fertilizer Units,4,4.0,61ef6c2e-4b10-4cad-b351-a4fff5afa801,21.0,Custom Fertilizer Units,5,5.0,181139ce-59de-4037-9367-bd3cfe7ea74f,10.0,Custom Fertilizer Units,6,6.0,Helena,John,4692,4.0,Herbicide Units,11394,1,1.0,11582,6.0,Herbicide Units,12558,2,2.0,10528,6.0,Herbicide Units,11150,3,3.0,11379,4.0,Herbicide Units,12273,4,4.0,11502,3.0,Herbicide Units,12457,5,5.0,9329,6.0,Herbicide Units,9751,6,6.0,11228,11.0,Herbicide Units,12039,7,7.0,591,11.0,Herbicide Units,591,8,8.0,310,349,349,1060,1060,1060,9359,4.0,Insecticide Units,9789,1,1.0,10003,3.0,Insecticide Units,10542,2,2.0,9571,6.0,Insecticide Units,10070,3,3.0,7496,4.0,Insecticide Units,7496,4,4.0,7642,3.0,Insecticide Units,7642,5,5.0,8390,4.0,Insecticide Units,8478,6,6.0,7589,6.0,Insecticide Units,8740,7,7.0,9562,6.0,Insecticide Units,10061,8,8.0,390,199,583,362,61,499,10300,4.0,Fungicide Units,10887,1,1.0,11388,7.0,Fungicide Units,12282,2,2.0,5444,4.0,Fungicide Units,5444,3,3.0,9195,4.0,Fungicide Units,9591,4,4.0,5266,3.0,Fungicide Units,5266,5,5.0,5345,4.0,Fungicide Units,5345,6,6.0,5423,4.0,Fungicide Units,5423,7,7.0,11581,2.0,Fungicide Units,12557,8,8.0,277,447,163,277,366,180,9164,4.0,Growth Regulator Units,9552,1,1.0,6023,2.0,Growth Regulator Units,10005,2,2.0,6058,4.5,Growth Regulator Units,6058,3,3.0,9176,4.0,Growth Regulator Units,9565,5,4.0,28,1,12.0,Plant Spacing Units,12.0,Row Spacing Units,5,5,4.0,Additive Units,1,1.0,120,6.0,Additive Units,2,2.0,892,4.0,Additive Units,3,3.0,510,6.0,Additive Units,4,4.0,730,4.5,Additive Units,5,5.0,731,6.0,Additive Units,6,6.0,13,1.0,Nitrogen Stabilizer Units,13,1,1.0,5,1.0,120.0,Total App. Units,4.0,REI Units,5.0,PHI/WHP Units,3.0,Plant Back Units,5.0,Band Width Units,10.0,4,68.0,Soil Temperature Units,1,Time Units,Yield Goal Units,40,5.0,Distance Units,lake,2,7,8,11,2,9.0,Wind Speed Units,68.0,75.0,Air Temperature Units,45,90.0,4.0,Delta T Units,0.01,Pre-App Rainfall Units,0.2,Post-App Rainfall Units,1,2,18,1,17,1,8.0,Speed Units,394,511,red,250.0,1158,1,280,1426,56,18.0,Nozzle Spacing Units,2,45.0,Spray Pressure Units,3,sprayer,24.0,Height Units,1,Frequency Units,4.0,400,Load Size Units,1,2,2,2,1,1,1,2,1,1,1,1,2,spraying co.,1,Austin,123456,here are some comments\r\n";
	private static final String CSV_ARRAY_WITH_HEADER = CSV_HEADER + "\r\n" + CSV_ARRAY_NO_HEADER;

	private static final String JSON_OBJECT_RESOURCE = "json-csv-object.json";
	private static final String CSV_OBJECT_NO_HEADER = "43edf03f-a0b7-4679-b2cd-6597c1241cfc,5fc63839-d4ce-4f34-8fe6-f58640ebca09,13,5559354a-0b5d-4cdc-9cd0-a985f446c62d,2017-09-08T16:20:04Z,2016-12-19T18:36:21Z,13,13,13,b788b9c4-1fe6-4007-9889-e6445ee3727b,85.14494974216365,7,6,152,6.0,Tillage Depth Units,16.0,Implement Width Units,5,11,712,64821,32000.0,Seeding Units,5,123456,6.0,Planting Depth Units,9240,5.0,Seed Treatment Units,9651,1,10549,4.0,Seed Treatment Units,11174,2,9412,5.0,Seed Treatment Units,9864,3,9587,4.5,Seed Treatment Units,10086,4,354,7,705,1,2,2,13,45.0,Fertilizer Rate Units,1,1.0,20,45.0,Fertilizer Rate Units,2,2.0,1581,41.0,Fertilizer Rate Units,3,3.0,977,13.0,Fertilizer Rate Units,4,4.0,1701,45.0,Fertilizer Rate Units,5,5.0,936,10.0,Fertilizer Rate Units,6,6.0,59f19149-08e8-4547-be1f-3e0bf80f75cf,25.0,Custom Fertilizer Units,1,1.0,0ae23dce-a6f9-4729-8097-dc48f9253773,12.0,Custom Fertilizer Units,2,2.0,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,30.0,Custom Fertilizer Units,3,3.0,3d95ceba-e129-43da-a7a7-dc31b119a166,10.0,Custom Fertilizer Units,4,4.0,61ef6c2e-4b10-4cad-b351-a4fff5afa801,21.0,Custom Fertilizer Units,5,5.0,181139ce-59de-4037-9367-bd3cfe7ea74f,10.0,Custom Fertilizer Units,6,6.0,Helena,John,4692,4.0,Herbicide Units,11394,1,1.0,11582,6.0,Herbicide Units,12558,2,2.0,10528,6.0,Herbicide Units,11150,3,3.0,11379,4.0,Herbicide Units,12273,4,4.0,11502,3.0,Herbicide Units,12457,5,5.0,9329,6.0,Herbicide Units,9751,6,6.0,11228,11.0,Herbicide Units,12039,7,7.0,591,11.0,Herbicide Units,591,8,8.0,310,349,349,1060,1060,1060,9359,4.0,Insecticide Units,9789,1,1.0,10003,3.0,Insecticide Units,10542,2,2.0,9571,6.0,Insecticide Units,10070,3,3.0,7496,4.0,Insecticide Units,7496,4,4.0,7642,3.0,Insecticide Units,7642,5,5.0,8390,4.0,Insecticide Units,8478,6,6.0,7589,6.0,Insecticide Units,8740,7,7.0,9562,6.0,Insecticide Units,10061,8,8.0,390,199,583,362,61,499,10300,4.0,Fungicide Units,10887,1,1.0,11388,7.0,Fungicide Units,12282,2,2.0,5444,4.0,Fungicide Units,5444,3,3.0,9195,4.0,Fungicide Units,9591,4,4.0,5266,3.0,Fungicide Units,5266,5,5.0,5345,4.0,Fungicide Units,5345,6,6.0,5423,4.0,Fungicide Units,5423,7,7.0,11581,2.0,Fungicide Units,12557,8,8.0,277,447,163,277,366,180,9164,4.0,Growth Regulator Units,9552,1,1.0,6023,2.0,Growth Regulator Units,10005,2,2.0,6058,4.5,Growth Regulator Units,6058,3,3.0,9176,4.0,Growth Regulator Units,9565,5,4.0,28,1,12.0,Plant Spacing Units,12.0,Row Spacing Units,5,5,4.0,Additive Units,1,1.0,120,6.0,Additive Units,2,2.0,892,4.0,Additive Units,3,3.0,510,6.0,Additive Units,4,4.0,730,4.5,Additive Units,5,5.0,731,6.0,Additive Units,6,6.0,13,1.0,Nitrogen Stabilizer Units,13,1,1.0,5,1.0,120.0,Total App. Units,4.0,REI Units,5.0,PHI/WHP Units,3.0,Plant Back Units,5.0,Band Width Units,10.0,4,68.0,Soil Temperature Units,1,Time Units,Yield Goal Units,40,5.0,Distance Units,lake,2,7,8,11,2,9.0,Wind Speed Units,68.0,75.0,Air Temperature Units,45,90.0,4.0,Delta T Units,0.01,Pre-App Rainfall Units,0.2,Post-App Rainfall Units,1,2,18,1,17,1,8.0,Speed Units,394,511,red,250.0,1158,1,280,1426,56,18.0,Nozzle Spacing Units,2,45.0,Spray Pressure Units,3,sprayer,24.0,Height Units,1,Frequency Units,4.0,400,Load Size Units,1,2,2,2,1,1,1,2,1,1,1,1,2,spraying co.,1,Austin,123456,here are some comments\r\n";
	private static final String CSV_OBJECT_WITH_HEADER = CSV_HEADER + "\r\n" + CSV_OBJECT_NO_HEADER;

	private static final String CSV_OBJECT_WITH_QUOTES = CSV_HEADER + "\r\n" + "43edf03f-a0b7-4679-b2cd-6597c1241cfc,5fc63839-d4ce-4f34-8fe6-f58640ebca09,13,5559354a-0b5d-4cdc-9cd0-a985f446c62d,2017-09-08T16:20:04Z,2016-12-19T18:36:21Z,13,13,13,b788b9c4-1fe6-4007-9889-e6445ee3727b,85.14494974216365,7,6,152,6.0,\"Tillage Depth \"\"Units\"\"\",16.0,\"Implement Width \"\"Units\"\"\",5,11,712,64821,32000.0,\"Seeding \"\"Units\"\"\",5,123456,6.0,\"Planting Depth \"\"Units\"\"\",9240,5.0,\"Seed Treatment \"\"Units\"\"\",9651,1,10549,4.0,\"Seed Treatment \"\"Units\"\"\",11174,2,9412,5.0,\"Seed Treatment \"\"Units\"\"\",9864,3,9587,4.5,\"Seed Treatment \"\"Units\"\"\",10086,4,354,7,705,1,2,2,13,45.0,\"Fertilizer Rate \"\"Units\"\"\",1,1.0,20,45.0,\"Fertilizer Rate \"\"Units\"\"\",2,2.0,1581,41.0,\"Fertilizer Rate \"\"Units\"\"\",3,3.0,977,13.0,\"Fertilizer Rate \"\"Units\"\"\",4,4.0,1701,45.0,\"Fertilizer Rate \"\"Units\"\"\",5,5.0,936,10.0,\"Fertilizer Rate \"\"Units\"\"\",6,6.0,59f19149-08e8-4547-be1f-3e0bf80f75cf,25.0,\"Custom Fertilizer \"\"Units\"\"\",1,1.0,0ae23dce-a6f9-4729-8097-dc48f9253773,12.0,\"Custom Fertilizer \"\"Units\"\"\",2,2.0,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,30.0,\"Custom Fertilizer \"\"Units\"\"\",3,3.0,3d95ceba-e129-43da-a7a7-dc31b119a166,10.0,\"Custom Fertilizer \"\"Units\"\"\",4,4.0,61ef6c2e-4b10-4cad-b351-a4fff5afa801,21.0,\"Custom Fertilizer \"\"Units\"\"\",5,5.0,181139ce-59de-4037-9367-bd3cfe7ea74f,10.0,\"Custom Fertilizer \"\"Units\"\"\",6,6.0,Helena,John,4692,4.0,\"Herbicide \"\"Units\"\"\",11394,1,1.0,11582,6.0,\"Herbicide \"\"Units\"\"\",12558,2,2.0,10528,6.0,\"Herbicide \"\"Units\"\"\",11150,3,3.0,11379,4.0,\"Herbicide \"\"Units\"\"\",12273,4,4.0,11502,3.0,\"Herbicide \"\"Units\"\"\",12457,5,5.0,9329,6.0,\"Herbicide \"\"Units\"\"\",9751,6,6.0,11228,11.0,\"Herbicide \"\"Units\"\"\",12039,7,7.0,591,11.0,\"Herbicide \"\"Units\"\"\",591,8,8.0,310,349,349,1060,1060,1060,9359,4.0,\"Insecticide \"\"Units\"\"\",9789,1,1.0,10003,3.0,\"Insecticide \"\"Units\"\"\",10542,2,2.0,9571,6.0,\"Insecticide \"\"Units\"\"\",10070,3,3.0,7496,4.0,\"Insecticide \"\"Units\"\"\",7496,4,4.0,7642,3.0,\"Insecticide \"\"Units\"\"\",7642,5,5.0,8390,4.0,\"Insecticide \"\"Units\"\"\",8478,6,6.0,7589,6.0,\"Insecticide \"\"Units\"\"\",8740,7,7.0,9562,6.0,\"Insecticide \"\"Units\"\"\",10061,8,8.0,390,199,583,362,61,499,10300,4.0,\"Fungicide \"\"Units\"\"\",10887,1,1.0,11388,7.0,\"Fungicide \"\"Units\"\"\",12282,2,2.0,5444,4.0,\"Fungicide \"\"Units\"\"\",5444,3,3.0,9195,4.0,\"Fungicide \"\"Units\"\"\",9591,4,4.0,5266,3.0,\"Fungicide \"\"Units\"\"\",5266,5,5.0,5345,4.0,\"Fungicide \"\"Units\"\"\",5345,6,6.0,5423,4.0,\"Fungicide \"\"Units\"\"\",5423,7,7.0,11581,2.0,\"Fungicide \"\"Units\"\"\",12557,8,8.0,277,447,163,277,366,180,9164,4.0,\"Growth Regulator \"\"Units\"\"\",9552,1,1.0,6023,2.0,\"Growth Regulator \"\"Units\"\"\",10005,2,2.0,6058,4.5,\"Growth Regulator \"\"Units\"\"\",6058,3,3.0,9176,4.0,\"Growth Regulator \"\"Units\"\"\",9565,5,4.0,28,1,12.0,\"Plant Spacing \"\"Units\"\"\",12.0,\"Row Spacing \"\"Units\"\"\",5,5,4.0,\"Additive \"\"Units\"\"\",1,1.0,120,6.0,\"Additive \"\"Units\"\"\",2,2.0,892,4.0,\"Additive \"\"Units\"\"\",3,3.0,510,6.0,\"Additive \"\"Units\"\"\",4,4.0,730,4.5,\"Additive \"\"Units\"\"\",5,5.0,731,6.0,\"Additive \"\"Units\"\"\",6,6.0,13,1.0,\"Nitrogen Stabilizer \"\"Units\"\"\",13,1,1.0,5,1.0,120.0,\"Total App. \"\"Units\"\"\",4.0,\"REI \"\"Units\"\"\",5.0,\"PHI/WHP \"\"Units\"\"\",3.0,\"Plant Back \"\"Units\"\"\",5.0,\"Band Width \"\"Units\"\"\",10.0,4,68.0,\"Soil Temperature \"\"Units\"\"\",1,\"Time \"\"Units\"\"\",\"Yield Goal \"\"Units\"\"\",40,5.0,\"Distance \"\"Units\"\"\",lake,2,7,8,11,2,9.0,\"Wind Speed \"\"Units\"\"\",68.0,75.0,\"Air Temperature \"\"Units\"\"\",45,90.0,4.0,\"Delta T \"\"Units\"\"\",0.01,\"Pre-App Rainfall \"\"Units\"\"\",0.2,\"Post-App Rainfall \"\"Units\"\"\",1,2,18,1,17,1,8.0,\"Speed \"\"Units\"\"\",394,511,red,250.0,1158,1,280,1426,56,18.0,\"Nozzle Spacing \"\"Units\"\"\",2,45.0,\"Spray Pressure \"\"Units\"\"\",3,sprayer,24.0,\"Height \"\"Units\"\"\",1,\"Frequency \"\"Units\"\"\",4.0,400,\"Load Size \"\"Units\"\"\",1,2,2,2,1,1,1,2,1,1,1,1,2,spraying co.,1,Austin,123456,here are some comments\r\n";
	private static final String CSV_OBJECT_WITH_COMMAS = CSV_HEADER + "\r\n" + "43edf03f-a0b7-4679-b2cd-6597c1241cfc,5fc63839-d4ce-4f34-8fe6-f58640ebca09,13,5559354a-0b5d-4cdc-9cd0-a985f446c62d,2017-09-08T16:20:04Z,2016-12-19T18:36:21Z,13,13,13,b788b9c4-1fe6-4007-9889-e6445ee3727b,85.14494974216365,7,6,152,6.0,\"Tillage Depth, Units\",16.0,\"Implement Width, Units\",5,11,712,64821,32000.0,\"Seeding, Units\",5,123456,6.0,\"Planting Depth, Units\",9240,5.0,\"Seed Treatment, Units\",9651,1,10549,4.0,\"Seed Treatment, Units\",11174,2,9412,5.0,\"Seed Treatment, Units\",9864,3,9587,4.5,\"Seed Treatment, Units\",10086,4,354,7,705,1,2,2,13,45.0,\"Fertilizer Rate, Units\",1,1.0,20,45.0,\"Fertilizer Rate, Units\",2,2.0,1581,41.0,\"Fertilizer Rate, Units\",3,3.0,977,13.0,\"Fertilizer Rate, Units\",4,4.0,1701,45.0,\"Fertilizer Rate, Units\",5,5.0,936,10.0,\"Fertilizer Rate, Units\",6,6.0,59f19149-08e8-4547-be1f-3e0bf80f75cf,25.0,\"Custom Fertilizer, Units\",1,1.0,0ae23dce-a6f9-4729-8097-dc48f9253773,12.0,\"Custom Fertilizer, Units\",2,2.0,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,30.0,\"Custom Fertilizer, Units\",3,3.0,3d95ceba-e129-43da-a7a7-dc31b119a166,10.0,\"Custom Fertilizer, Units\",4,4.0,61ef6c2e-4b10-4cad-b351-a4fff5afa801,21.0,\"Custom Fertilizer, Units\",5,5.0,181139ce-59de-4037-9367-bd3cfe7ea74f,10.0,\"Custom Fertilizer, Units\",6,6.0,Helena,John,4692,4.0,\"Herbicide, Units\",11394,1,1.0,11582,6.0,\"Herbicide, Units\",12558,2,2.0,10528,6.0,\"Herbicide, Units\",11150,3,3.0,11379,4.0,\"Herbicide, Units\",12273,4,4.0,11502,3.0,\"Herbicide, Units\",12457,5,5.0,9329,6.0,\"Herbicide, Units\",9751,6,6.0,11228,11.0,\"Herbicide, Units\",12039,7,7.0,591,11.0,\"Herbicide, Units\",591,8,8.0,310,349,349,1060,1060,1060,9359,4.0,\"Insecticide, Units\",9789,1,1.0,10003,3.0,\"Insecticide, Units\",10542,2,2.0,9571,6.0,\"Insecticide, Units\",10070,3,3.0,7496,4.0,\"Insecticide, Units\",7496,4,4.0,7642,3.0,\"Insecticide, Units\",7642,5,5.0,8390,4.0,\"Insecticide, Units\",8478,6,6.0,7589,6.0,\"Insecticide, Units\",8740,7,7.0,9562,6.0,\"Insecticide, Units\",10061,8,8.0,390,199,583,362,61,499,10300,4.0,\"Fungicide, Units\",10887,1,1.0,11388,7.0,\"Fungicide, Units\",12282,2,2.0,5444,4.0,\"Fungicide, Units\",5444,3,3.0,9195,4.0,\"Fungicide, Units\",9591,4,4.0,5266,3.0,\"Fungicide, Units\",5266,5,5.0,5345,4.0,\"Fungicide, Units\",5345,6,6.0,5423,4.0,\"Fungicide, Units\",5423,7,7.0,11581,2.0,\"Fungicide, Units\",12557,8,8.0,277,447,163,277,366,180,9164,4.0,\"Growth Regulator, Units\",9552,1,1.0,6023,2.0,\"Growth Regulator, Units\",10005,2,2.0,6058,4.5,\"Growth Regulator, Units\",6058,3,3.0,9176,4.0,\"Growth Regulator, Units\",9565,5,4.0,28,1,12.0,\"Plant Spacing, Units\",12.0,\"Row Spacing, Units\",5,5,4.0,\"Additive, Units\",1,1.0,120,6.0,\"Additive, Units\",2,2.0,892,4.0,\"Additive, Units\",3,3.0,510,6.0,\"Additive, Units\",4,4.0,730,4.5,\"Additive, Units\",5,5.0,731,6.0,\"Additive, Units\",6,6.0,13,1.0,\"Nitrogen Stabilizer, Units\",13,1,1.0,5,1.0,120.0,\"Total App., Units\",4.0,\"REI, Units\",5.0,\"PHI/WHP, Units\",3.0,\"Plant Back, Units\",5.0,\"Band Width, Units\",10.0,4,68.0,\"Soil Temperature, Units\",1,\"Time, Units\",\"Yield Goal, Units\",40,5.0,\"Distance, Units\",lake,2,7,8,11,2,9.0,\"Wind Speed, Units\",68.0,75.0,\"Air Temperature, Units\",45,90.0,4.0,\"Delta T, Units\",0.01,\"Pre-App Rainfall, Units\",0.2,\"Post-App Rainfall, Units\",1,2,18,1,17,1,8.0,\"Speed, Units\",394,511,red,250.0,1158,1,280,1426,56,18.0,\"Nozzle Spacing, Units\",2,45.0,\"Spray Pressure, Units\",3,sprayer,24.0,\"Height, Units\",1,\"Frequency, Units\",4.0,400,\"Load Size, Units\",1,2,2,2,1,1,1,2,1,1,1,1,2,spraying co.,1,Austin,123456,here are some comments\r\n";
	private static final String CSV_OBJECT_WITH_NEWLINES = CSV_HEADER + "\r\n" + "43edf03f-a0b7-4679-b2cd-6597c1241cfc,5fc63839-d4ce-4f34-8fe6-f58640ebca09,13,5559354a-0b5d-4cdc-9cd0-a985f446c62d,2017-09-08T16:20:04Z,2016-12-19T18:36:21Z,13,13,13,b788b9c4-1fe6-4007-9889-e6445ee3727b,85.14494974216365,7,6,152,6.0,\"Tillage Depth\n Units\",16.0,\"Implement Width\n Units\",5,11,712,64821,32000.0,\"Seeding\n Units\",5,123456,6.0,\"Planting Depth\n Units\",9240,5.0,\"Seed Treatment\n Units\",9651,1,10549,4.0,\"Seed Treatment\n Units\",11174,2,9412,5.0,\"Seed Treatment\n Units\",9864,3,9587,4.5,\"Seed Treatment\n Units\",10086,4,354,7,705,1,2,2,13,45.0,\"Fertilizer Rate\n Units\",1,1.0,20,45.0,\"Fertilizer Rate\n Units\",2,2.0,1581,41.0,\"Fertilizer Rate\n Units\",3,3.0,977,13.0,\"Fertilizer Rate\n Units\",4,4.0,1701,45.0,\"Fertilizer Rate\n Units\",5,5.0,936,10.0,\"Fertilizer Rate\n Units\",6,6.0,59f19149-08e8-4547-be1f-3e0bf80f75cf,25.0,\"Custom Fertilizer\n Units\",1,1.0,0ae23dce-a6f9-4729-8097-dc48f9253773,12.0,\"Custom Fertilizer\n Units\",2,2.0,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,30.0,\"Custom Fertilizer\n Units\",3,3.0,3d95ceba-e129-43da-a7a7-dc31b119a166,10.0,\"Custom Fertilizer\n Units\",4,4.0,61ef6c2e-4b10-4cad-b351-a4fff5afa801,21.0,\"Custom Fertilizer\n Units\",5,5.0,181139ce-59de-4037-9367-bd3cfe7ea74f,10.0,\"Custom Fertilizer\n Units\",6,6.0,Helena,John,4692,4.0,\"Herbicide\n Units\",11394,1,1.0,11582,6.0,\"Herbicide\n Units\",12558,2,2.0,10528,6.0,\"Herbicide\n Units\",11150,3,3.0,11379,4.0,\"Herbicide\n Units\",12273,4,4.0,11502,3.0,\"Herbicide\n Units\",12457,5,5.0,9329,6.0,\"Herbicide\n Units\",9751,6,6.0,11228,11.0,\"Herbicide\n Units\",12039,7,7.0,591,11.0,\"Herbicide\n Units\",591,8,8.0,310,349,349,1060,1060,1060,9359,4.0,\"Insecticide\n Units\",9789,1,1.0,10003,3.0,\"Insecticide\n Units\",10542,2,2.0,9571,6.0,\"Insecticide\n Units\",10070,3,3.0,7496,4.0,\"Insecticide\n Units\",7496,4,4.0,7642,3.0,\"Insecticide\n Units\",7642,5,5.0,8390,4.0,\"Insecticide\n Units\",8478,6,6.0,7589,6.0,\"Insecticide\n Units\",8740,7,7.0,9562,6.0,\"Insecticide\n Units\",10061,8,8.0,390,199,583,362,61,499,10300,4.0,\"Fungicide\n Units\",10887,1,1.0,11388,7.0,\"Fungicide\n Units\",12282,2,2.0,5444,4.0,\"Fungicide\n Units\",5444,3,3.0,9195,4.0,\"Fungicide\n Units\",9591,4,4.0,5266,3.0,\"Fungicide\n Units\",5266,5,5.0,5345,4.0,\"Fungicide\n Units\",5345,6,6.0,5423,4.0,\"Fungicide\n Units\",5423,7,7.0,11581,2.0,\"Fungicide\n Units\",12557,8,8.0,277,447,163,277,366,180,9164,4.0,\"Growth Regulator\n Units\",9552,1,1.0,6023,2.0,\"Growth Regulator\n Units\",10005,2,2.0,6058,4.5,\"Growth Regulator\n Units\",6058,3,3.0,9176,4.0,\"Growth Regulator\n Units\",9565,5,4.0,28,1,12.0,\"Plant Spacing\n Units\",12.0,\"Row Spacing\n Units\",5,5,4.0,\"Additive\n Units\",1,1.0,120,6.0,\"Additive\n Units\",2,2.0,892,4.0,\"Additive\n Units\",3,3.0,510,6.0,\"Additive\n Units\",4,4.0,730,4.5,\"Additive\n Units\",5,5.0,731,6.0,\"Additive\n Units\",6,6.0,13,1.0,\"Nitrogen Stabilizer\n Units\",13,1,1.0,5,1.0,120.0,\"Total App.\n Units\",4.0,\"REI\n Units\",5.0,\"PHI/WHP\n Units\",3.0,\"Plant Back\n Units\",5.0,\"Band Width\n Units\",10.0,4,68.0,\"Soil Temperature\n Units\",1,\"Time\n Units\",\"Yield Goal\n Units\",40,5.0,\"Distance\n Units\",lake,2,7,8,11,2,9.0,\"Wind Speed\n Units\",68.0,75.0,\"Air Temperature\n Units\",45,90.0,4.0,\"Delta T\n Units\",0.01,\"Pre-App Rainfall\n Units\",0.2,\"Post-App Rainfall\n Units\",1,2,18,1,17,1,8.0,\"Speed\n Units\",394,511,red,250.0,1158,1,280,1426,56,18.0,\"Nozzle Spacing\n Units\",2,45.0,\"Spray Pressure\n Units\",3,sprayer,24.0,\"Height\n Units\",1,\"Frequency\n Units\",4.0,400,\"Load Size\n Units\",1,2,2,2,1,1,1,2,1,1,1,1,2,spraying co.,1,Austin,123456,here are some comments\r\n";

	private static final String JSON_MINIMAL_RESOURCE = "json-csv-minimal.json";
	private static final String CSV_MINIMAL = "43edf03f-a0b7-4679-b2cd-6597c1241cfc,5fc63839-d4ce-4f34-8fe6-f58640ebca09,,5559354a-0b5d-4cdc-9cd0-a985f446c62d,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,here are some comments\r\n";
	private static final String JSON_INVALID_RESOURCE = "json-csv-invalid.json";
	private static final String CSV_NONE = "";

	public JsonToFixedCSVTest()
	{
		super("Json to fixed CSV service test");
	}

	/**
	 * Test that a JSON array becomes several lines on CSV,
	 * and displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testServiceWithArrayWithHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_ARRAY_RESOURCE);
		JsonToFixedCSV service = getService(true, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_ARRAY_WITH_HEADER, message.getContent());
	}

	/**
	 * Test that a JSON array becomes several lines on CSV,
	 * and not displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testServiceWithArrayNoHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_ARRAY_RESOURCE);
		JsonToFixedCSV service = getService(false, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_ARRAY_NO_HEADER, message.getContent());
	}

	/**
	 * Test that a JSON object becomes CSV data,
	 * and displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testServiceWithObjectWithHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		JsonToFixedCSV service = getService(true, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_WITH_HEADER, message.getContent());
	}

	/**
	 * Test that a JSON object becomes CSV data,
	 * and not displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testServiceWithObjectNoHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		JsonToFixedCSV service = getService(false, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_NO_HEADER, message.getContent());
	}


	/**
	 * Test that a JSON object becomes CSV data,
	 * with quotes in the JSON, which should be escaped in the CSV
	 * data.
	 *
	 * @throws Exception
	 */
	@Test
	public void testServiceWithQuotes() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		message.setContent(message.getContent().replace("Units", "\\\"Units\\\""), message.getContentEncoding());
		JsonToFixedCSV service = getService(true, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_WITH_QUOTES, message.getContent());
	}

	/**
	 * Test that a JSON object becomes CSV data,
	 * with commas in the JSON, which should be escaped in the CSV
	 * data.
	 *
	 * @throws Exception
	 */
	@Test
	public void testServiceWithCommas() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		message.setContent(message.getContent().replace(" Units", ", Units"), message.getContentEncoding());
		JsonToFixedCSV service = getService(true, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_WITH_COMMAS, message.getContent());
	}

	/**
	 * Test that a JSON object becomes CSV data,
	 * with newlines in the JSON, which should be escaped in the CSV
	 * data.
	 *
	 * @throws Exception
	 */
	@Test
	public void testServiceWithNewlines() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		message.setContent(message.getContent().replace(" Units", "\\n Units"), message.getContentEncoding());
		JsonToFixedCSV service = getService(true, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_WITH_NEWLINES, message.getContent());
	}

	/**
	 * Test that the service behaves as expected if limited JSON is
	 * given to it.
	 *
	 * @throws Exception
	 */
	@Test
	public void testMinimalJson() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_MINIMAL_RESOURCE);
		JsonToFixedCSV service = getService(false, CSV_HEADER);

		execute(service, message);

		Assert.assertFalse(service.getShowHeader());
		Assert.assertEquals(CSV_MINIMAL, message.getContent());
	}

	/**
	 * Test that the service behaves as expected if bad JSON is given
	 * to it.
	 *
	 * @throws Exception
	 */
	@Test
	public void testNotJson() throws Exception
	{

		AdaptrisMessage message = getMessage(JSON_INVALID_RESOURCE);
		JsonToFixedCSV service = getService(false, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(CSV_NONE, message.getContent());
	}

	private AdaptrisMessage getMessage(String resource) throws IOException
	{
		return AdaptrisMessageFactory.getDefaultInstance().newMessage(IOUtils.toString(getClass().getResourceAsStream(resource), "UTF-8"));
	}

	private JsonToFixedCSV getService(boolean showHeader, String header)
	{
		JsonToFixedCSV service = (JsonToFixedCSV)retrieveObjectForSampleConfig();
		service.setShowHeader(showHeader);
		service.setCsvHeader(header);
		return service;
	}

	@Override
	protected Object retrieveObjectForSampleConfig()
	{
		return new JsonToFixedCSV();
	}
}
