package com.adaptris.core.services;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.Service;
import com.adaptris.core.ServiceCase;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JsonToFixedCSVServiceTest extends ServiceCase
{
	private static final String CSV_HEADERS = "PlantBack,AdditiveID4,AdditiveID3,WindSpeed,EditorID,AdditiveID5,CustomBlendFertilizerMixOrder1,AdditiveID0,CustomBlendFertilizerMixOrder0,NitrogenStabilizerID,CustomBlendFertilizerMixOrder3,AdditiveID2,CustomBlendFertilizerMixOrder2,AdditiveID1,TractorWithCab,EndAMorPMID,ProductSourceContactPerson,TimeUntilIncorporationUnit,CommercialFertilizerMixOrder1,CustomBlendFertilizerMixOrder5,AverageSpeedUnit,CommercialFertilizerMixOrder0,CustomBlendFertilizerMixOrder4,CommercialFertilizerMixOrder5,CommercialFertilizerMixOrder4,CommercialFertilizerMixOrder3,CommercialFertilizerMixOrder2,SeedTreatmentBatchNumber2,SeedTreatmentBatchNumber3,SeedTreatmentBatchNumber0,SeedTreatmentBatchNumber1,NitrogenStabilizerRegistrationCodeID,ProductSource,PlantingDepth,HerbicideID7,HoursOfTractorOperation,HerbicideID6,HerbicideID1,HerbicideID0,HerbicideID3,HerbicideID2,HerbicideID5,HerbicideID4,NozzleModel,REI,ModifiedOn,AirTemperatureUnit,NozzlePressureUnit,PotentialSensitiveAreaID,AdditiveRateUnit4,AdditiveRateUnit5,AdditiveRateUnit0,AdditiveRateUnit1,AdditiveRateUnit2,FieldID,AdditiveRateUnit3,SecondaryTargetDiseaseID2,TractorModelID,PrimaryTargetDiseaseID2,PrimaryTargetDiseaseID0,SecondaryTargetDiseaseID1,PlanterClass,PrimaryTargetDiseaseID1,SecondaryTargetDiseaseID0,TractorName,REIUnit,PercentRelativeHumidityInCanopy,SecondaryTargetWeedID0,PHI_WHP,BufferStripTypeID,PostAppRainfallAmountLast24HrUnit,SecondaryTargetWeedID1,SecondaryTargetWeedID2,CommercialFertilizerBatchNumber4,CommercialFertilizerBatchNumber3,CommercialFertilizerBatchNumber5,CommercialFertilizerBatchNumber0,SurfaceWaterName,CommercialFertilizerBatchNumber2,CommercialFertilizerBatchNumber1,CommercialFertilizerID2,CommercialFertilizerID1,CommercialFertilizerID0,CommercialFertilizerID5,CommercialFertilizerID4,CommercialFertilizerID3,TotalApplicationCarrierID,ApplicationEquipmentModelID,BandWidth,TotalApplicationRateUnit,VarietyHybridID,RiskAssessmentUndertaken,CreatorID,PHI_WHPUnit,NozzleSpacingUnit,RespiratorWorn,PlantBackUnit,AdditiveMixOrder1,ID,AdditiveMixOrder0,AdditiveMixOrder3,AdditiveMixOrder2,AdditiveMixOrder5,AdditiveMixOrder4,PlantSpacingUnit,NozzleSpacing,FungicideRegistrationCodeID3,FungicideRegistrationCodeID4,FungicideRegistrationCodeID1,FungicideRegistrationCodeID2,StartAMorPMID,FungicideRegistrationCodeID7,GrowthRegulatorBatchNumber0,FungicideRegistrationCodeID5,FungicideRegistrationCodeID6,GrowthRegulatorBatchNumber3,GogglesWorn,GrowthRegulatorBatchNumber1,GrowthRegulatorBatchNumber2,FungicideRegistrationCodeID0,CalibrationFrequency,EndTime,GrowthRegulatorRateUnit3,GrowthRegulatorRateUnit2,CustomOperationFirmName,GrowthRegulatorRateUnit1,GrowthRegulatorRateUnit0,AirTemperature,GlovesWorn,ProductReleaseHeight,NitrogenStabilizerRateUnit,PercentCropResidue,TimeUntilIncorporation,SyncID,TotalFieldArea,HeadProtectionWorn,GrowthRegulatorRate3,GrowthRegulatorRate2,GrowthRegulatorRate1,GrowthRegulatorRate0,SeedCompanyID,SetBackAdheredID,TillageDirection,CustomBlendFertilizerBatchNumber4,CustomBlendFertilizerBatchNumber3,CustomBlendFertilizerBatchNumber2,HerbicideRateUnit7,CustomBlendFertilizerBatchNumber1,CustomBlendFertilizerBatchNumber5,HerbicideRateUnit1,HerbicideRateUnit2,HerbicideRateUnit0,CustomBlendFertilizerBatchNumber0,HerbicideRateUnit5,HerbicideRateUnit6,HerbicideRateUnit3,HerbicideRateUnit4,NitrogenStabilizerRate,ApplicatorTypeID,PrimaryTargetWeedID0,PrimaryTargetWeedID2,PrimaryTargetWeedID1,FungicideRateUnit2,FungicideRateUnit3,FungicideRateUnit0,FungicideRateUnit1,FungicideRateUnit6,FungicideRateUnit7,LabelRead,FungicideRateUnit4,FungicideRateUnit5,TotalApplicationMixOrder,SoilTemperature,TotalNumberOfLoads,WindSpeedUnit,FungicideRegistrationMixOrder7,FungicideRegistrationMixOrder5,FungicideRegistrationMixOrder6,FungicideRegistrationMixOrder3,FungicideRegistrationMixOrder4,GrowthRegulatorID2,GrowthRegulatorID1,CreatedOn,GrowthRegulatorID3,GrowthRegulatorID0,NeighborsNotified,RowSpacingUnit,Comments,CommercialFertilizerUnit3,FungicideRegistrationMixOrder1,CommercialFertilizerUnit2,FungicideRegistrationMixOrder2,CommercialFertilizerUnit1,CommercialFertilizerUnit0,FungicideRegistrationMixOrder0,CommercialFertilizerUnit5,CommercialFertilizerUnit4,InsecticideRegistrationCode4,InsecticideRegistrationCode5,InsecticideRegistrationCode2,InsecticideRegistrationCode3,InsecticideRegistrationCode0,PlantSpacing,InsecticideRegistrationCode1,CustomOperation,InsecticideRegistrationCode6,InsecticideRegistrationCode7,HerbicideRegistrationBatchNumber7,HerbicideRate1,HerbicideRate2,InsecticideID7,HerbicideRate0,InsecticideID4,InsecticideID3,InsecticideID6,InsecticideID5,TileInspection,InsecticideID0,CommercialFertilizerRate4,HerbicideRate7,InsecticideID2,CommercialFertilizerRate5,InsecticideID1,HerbicideRate5,HerbicideRate6,HerbicideRate3,HerbicideRate4,HerbicideRegistrationBatchNumber2,HerbicideRegistrationBatchNumber1,HerbicideRegistrationBatchNumber0,CommercialFertilizerRate2,HerbicideRegistrationBatchNumber6,CropPurposeID,CommercialFertilizerRate3,HerbicideRegistrationBatchNumber5,CommercialFertilizerRate0,HerbicideRegistrationBatchNumber4,CommercialFertilizerRate1,HerbicideRegistrationBatchNumber3,SecondaryTargetInsectID2,SecondaryTargetInsectID0,FungicideRegistrationBatchNumber0,SecondaryTargetInsectID1,FungicideRegistrationBatchNumber1,CustomBlendFertilizerUnit4,FungicideRegistrationBatchNumber6,CustomBlendFertilizerUnit5,FungicideRegistrationBatchNumber7,CustomBlendFertilizerUnit2,CustomBlendFertilizerUnit3,CustomBlendFertilizerUnit0,FungicideRegistrationBatchNumber2,CustomBlendFertilizerUnit1,FungicideRegistrationBatchNumber3,FungicideRegistrationBatchNumber4,FungicideRegistrationBatchNumber5,SeedingRate,DoubleCropID,SoilConditionID,SecondaryImplement,NitrogenStabilizerMixOrder,OperatorLicenseNumber,EquipmentName,PrimaryImplement,DeltaT,AdditiveBatchNumber0,InsecticideRegistrationMixOrder0,InsecticideRegistrationMixOrder1,AdditiveBatchNumber2,InsecticideRegistrationMixOrder2,AdditiveBatchNumber1,AdditiveBatchNumber4,AdditiveBatchNumber3,AdditiveBatchNumber5,ImplementWidth,AverageLoadSizeUnit,StartTime,InsecticideRegistrationBatchNumber1,InsecticideRegistrationBatchNumber0,InsecticideRegistrationBatchNumber7,InsecticideRegistrationBatchNumber6,RowOrientationID,InsecticideRegistrationBatchNumber3,InsecticideRegistrationBatchNumber2,InsecticideRegistrationBatchNumber5,InsecticideRegistrationBatchNumber4,TotalApplicationRate,ApplicationEquipmentClassID,PlanterModelID,Operator,CustomBlendFertilizerMeasure4,CustomBlendFertilizerMeasure5,ImplementUnit,CustomBlendFertilizerMeasure0,CustomBlendFertilizerMeasure1,CustomBlendFertilizerMeasure2,CustomBlendFertilizerMeasure3,LotNumber,BootsWorn,TractorCompanyID,PercentRelativeHumidity,SeedingRateUnit,OverallsWorn,WindDirectionID,GrowthRegulatorMixOrder2,GrowthRegulatorMixOrder3,GrowthRegulatorMixOrder0,GrowthRegulatorMixOrder1,BandWidthUnit,FungicideRate3,UsedGPSID,FungicideRate2,FungicideRate1,FungicideRate0,ProductReleaseHeightUnit,CropID,FungicideRate7,ApplicationTimingID,FungicideRate6,FungicideRate5,FungicideRate4,FarmID,RowSpacing,TillageUnit,BandingPercentage,PreAppRainfallAmountLast24HrUnit,PlanterCompanyID,SeedTreatmentRegistrationCode3,ReplantID,DeltaTUnit,AverageSpeed,SeedTreatmentRegistrationCode1,SeedTreatmentRegistrationCode2,CalibrationFrequencyUnit,SeedTreatmentRegistrationCode0,ClosestSurfaceWaterDistance,ApplicationMethodID,ApronWorn,PlantingDepthUnit,FungicideID4,AdditiveRate2,FungicideID3,AdditiveRate3,FungicideID6,AdditiveRate0,FungicideID5,AdditiveRate1,InsecticideUnit2,InsecticideUnit3,FungicideID7,AirTemperatureInCanopy,DropletSize,InsecticideUnit0,InsecticideUnit1,InsecticideUnit6,InsecticideUnit7,InsecticideUnit4,InsecticideUnit5,SoilTemperatureUnit,SeedTreatmentID1,AdditiveRate4,SeedTreatmentID0,AdditiveRate5,SeedTreatmentID3,CustomBlendFertilizerID2,SeedTreatmentID2,CustomBlendFertilizerID1,CustomBlendFertilizerID4,CustomBlendFertilizerID3,CustomBlendFertilizerID0,InsecticideRegistrationMixOrder7,PrimaryTargetInsectID0,PrimaryTargetInsectID1,HourlyOutletInspectionID,GrowerId,InsecticideRegistrationMixOrder3,FungicideID0,CustomBlendFertilizerID5,InsecticideRegistrationMixOrder4,InsecticideRegistrationMixOrder5,PrimaryTargetInsectID2,FungicideID2,InsecticideRegistrationMixOrder6,FungicideID1,HerbicideRegistrationMixOrder0,HerbicideRegistrationMixOrder1,HerbicideRegistrationMixOrder2,HerbicideRegistrationMixOrder3,HerbicideRegistrationMixOrder4,NozzleCompany,HerbicideRegistrationMixOrder5,YieldGoalUnit,HerbicideRegistrationMixOrder6,HerbicideRegistrationMixOrder7,OverallWeatherID,InsecticideRate2,InsecticideRate3,InsecticideRate0,ClosestSurfaceWaterDistanceUnit,InsecticideRate1,InsecticideRate6,InsecticideRate7,InsecticideRate4,InsecticideRate5,PostAppRainfallAmountLast24Hr,NozzlePressure,SDSRead,TillageStyle,AdminID,GrowthRegulatorRegistrationCode3,GrowthRegulatorRegistrationCode2,GrowthRegulatorRegistrationCode1,GrowthRegulatorRegistrationCode0,HerbicideRegistrationCodeID6,HerbicideRegistrationCodeID7,HerbicideRegistrationCodeID4,HerbicideRegistrationCodeID5,ApplicationEquipmentCompanyID,HerbicideRegistrationCodeID2,HerbicideRegistrationCodeID3,HerbicideRegistrationCodeID0,HerbicideRegistrationCodeID1,BinRunSeedID,SeedTreatmentRate1,SeedTreatmentRate2,AverageLoadSize,SeedTreatmentRate0,PreAppRainfallAmountLast24Hr,SeedTreatmentRate3,TillageDepth,SeedTreatmentUnit1,SeedTreatmentUnit2,SeedTreatmentUnit0,NitrogenStabilizerBatchNumber,SeedTreatmentUnit3";

	private static final String JSON_ARRAY_RESOURCE = "";
	private static final String CSV_ARRAY_NO_HEADERS = "3.0,730,510,9.0,13,731,2.0,5,1.0,13,4.0,892,3.0,120,1,1,John,Time Units,2.0,6.0,Speed Units,1.0,5.0,6.0,5.0,4.0,3.0,3,4,1,2,13,Helena,6.0,591,250.0,11228,11582,4692,11379,10528,9329,11502,56,4.0,2017-09-08T16:20:04Z,Air Temperature Units,Spray Pressure Units,7,Additive Units,Additive Units,Additive Units,Additive Units,Additive Units,b788b9c4-1fe6-4007-9889-e6445ee3727b,Additive Units,180,511,163,277,366,7,447,277,red,REI Units,90.0,1060,5.0,8,Post-App Rainfall Units,1060,1060,5,4,6,1,lake,3,2,1581,20,13,936,1701,977,5,280,5.0,Total App. Units,64821,1,13,PHI/WHP Units,Nozzle Spacing Units,1,Plant Back Units,2.0,5559354a-0b5d-4cdc-9cd0-a985f446c62d,1.0,4.0,3.0,6.0,5.0,Plant Spacing Units,18.0,9591,5266,12282,5444,1,12557,1,5345,5423,5,1,2,3,10887,1,17,Growth Regulator Units,Growth Regulator Units,spraying co.,Growth Regulator Units,Growth Regulator Units,68.0,2,24.0,Nitrogen Stabilizer Units,40,1,13,85.14494974216365,1,4.0,4.5,2.0,4.0,712,2,5,5,4,3,Herbicide Units,2,6,Herbicide Units,Herbicide Units,Herbicide Units,1,Herbicide Units,Herbicide Units,Herbicide Units,Herbicide Units,1.0,3,310,349,349,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,1,Fungicide Units,Fungicide Units,1.0,68.0,4.0,Wind Speed Units,8.0,6.0,7.0,4.0,5.0,6058,6023,2016-12-19T18:36:21Z,9176,9164,2,Row Spacing Units,here are some comments,Fertilizer Rate Units,2.0,Fertilizer Rate Units,3.0,Fertilizer Rate Units,Fertilizer Rate Units,1.0,Fertilizer Rate Units,Fertilizer Rate Units,7642,8478,10070,7496,9789,12.0,10542,2,8740,10061,8,6.0,6.0,9562,4.0,7642,7496,7589,8390,1,9359,45.0,11.0,9571,10.0,10003,6.0,11.0,4.0,3.0,3,2,1,41.0,7,5,13.0,6,45.0,5,45.0,4,499,362,1,61,2,Custom Fertilizer Units,7,Custom Fertilizer Units,8,Custom Fertilizer Units,Custom Fertilizer Units,Custom Fertilizer Units,3,Custom Fertilizer Units,4,5,6,32000.0,2,4,152,1.0,123456,sprayer,6,4.0,1,1.0,2.0,3,3.0,2,5,4,6,16.0,Load Size Units,18,2,1,8,7,5,4,3,6,5,120.0,1,705,Austin,21.0,10.0,Implement Width Units,25.0,12.0,30.0,10.0,123456,2,394,45,Seeding Units,1,2,3.0,4.0,1.0,2.0,Band Width Units,4.0,1,4.0,7.0,4.0,Height Units,11,2.0,1,4.0,4.0,3.0,5fc63839-d4ce-4f34-8fe6-f58640ebca09,12.0,Tillage Depth Units,10.0,Pre-App Rainfall Units,354,10086,1,Delta T Units,8.0,11174,9864,Frequency Units,9651,5.0,28,2,Planting Depth Units,5266,4.0,9195,6.0,5423,4.0,5345,6.0,Insecticide Units,Insecticide Units,11581,75.0,2,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Soil Temperature Units,10549,4.5,9240,6.0,9587,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,9412,0ae23dce-a6f9-4729-8097-dc48f9253773,61ef6c2e-4b10-4cad-b351-a4fff5afa801,3d95ceba-e129-43da-a7a7-dc31b119a166,59f19149-08e8-4547-be1f-3e0bf80f75cf,8.0,390,199,2,43edf03f-a0b7-4679-b2cd-6597c1241cfc,4.0,10300,181139ce-59de-4037-9367-bd3cfe7ea74f,5.0,6.0,583,5444,7.0,11388,1.0,2.0,3.0,4.0,5.0,1426,6.0,Yield Goal Units,7.0,8.0,11,6.0,4.0,4.0,Distance Units,3.0,6.0,6.0,3.0,4.0,0.2,45.0,1,7,13,9565,6058,10005,9552,12039,591,12457,9751,1158,11150,12273,11394,12558,2,4.0,5.0,400,5.0,0.01,4.5,6.0,Seed Treatment Units,Seed Treatment Units,Seed Treatment Units,1,Seed Treatment Units\n" +
			"3.0,730,510,9.0,13,731,2.0,5,1.0,13,4.0,892,3.0,120,1,1,John,Time Units,2.0,6.0,Speed Units,1.0,5.0,6.0,5.0,4.0,3.0,3,4,1,2,13,Helena,6.0,591,250.0,11228,11582,4692,11379,10528,9329,11502,56,4.0,2017-09-08T16:20:04Z,Air Temperature Units,Spray Pressure Units,7,Additive Units,Additive Units,Additive Units,Additive Units,Additive Units,b788b9c4-1fe6-4007-9889-e6445ee3727b,Additive Units,180,511,163,277,366,7,447,277,red,REI Units,90.0,1060,5.0,8,Post-App Rainfall Units,1060,1060,5,4,6,1,lake,3,2,1581,20,13,936,1701,977,5,280,5.0,Total App. Units,64821,1,13,PHI/WHP Units,Nozzle Spacing Units,1,Plant Back Units,2.0,5559354a-0b5d-4cdc-9cd0-a985f446c62d,1.0,4.0,3.0,6.0,5.0,Plant Spacing Units,18.0,9591,5266,12282,5444,1,12557,1,5345,5423,5,1,2,3,10887,1,17,Growth Regulator Units,Growth Regulator Units,spraying co.,Growth Regulator Units,Growth Regulator Units,68.0,2,24.0,Nitrogen Stabilizer Units,40,1,13,85.14494974216365,1,4.0,4.5,2.0,4.0,712,2,5,5,4,3,Herbicide Units,2,6,Herbicide Units,Herbicide Units,Herbicide Units,1,Herbicide Units,Herbicide Units,Herbicide Units,Herbicide Units,1.0,3,310,349,349,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,1,Fungicide Units,Fungicide Units,1.0,68.0,4.0,Wind Speed Units,8.0,6.0,7.0,4.0,5.0,6058,6023,2016-12-19T18:36:21Z,9176,9164,2,Row Spacing Units,here are some comments,Fertilizer Rate Units,2.0,Fertilizer Rate Units,3.0,Fertilizer Rate Units,Fertilizer Rate Units,1.0,Fertilizer Rate Units,Fertilizer Rate Units,7642,8478,10070,7496,9789,12.0,10542,2,8740,10061,8,6.0,6.0,9562,4.0,7642,7496,7589,8390,1,9359,45.0,11.0,9571,10.0,10003,6.0,11.0,4.0,3.0,3,2,1,41.0,7,5,13.0,6,45.0,5,45.0,4,499,362,1,61,2,Custom Fertilizer Units,7,Custom Fertilizer Units,8,Custom Fertilizer Units,Custom Fertilizer Units,Custom Fertilizer Units,3,Custom Fertilizer Units,4,5,6,32000.0,2,4,152,1.0,123456,sprayer,6,4.0,1,1.0,2.0,3,3.0,2,5,4,6,16.0,Load Size Units,18,2,1,8,7,5,4,3,6,5,120.0,1,705,Austin,21.0,10.0,Implement Width Units,25.0,12.0,30.0,10.0,123456,2,394,45,Seeding Units,1,2,3.0,4.0,1.0,2.0,Band Width Units,4.0,1,4.0,7.0,4.0,Height Units,11,2.0,1,4.0,4.0,3.0,5fc63839-d4ce-4f34-8fe6-f58640ebca09,12.0,Tillage Depth Units,10.0,Pre-App Rainfall Units,354,10086,1,Delta T Units,8.0,11174,9864,Frequency Units,9651,5.0,28,2,Planting Depth Units,5266,4.0,9195,6.0,5423,4.0,5345,6.0,Insecticide Units,Insecticide Units,11581,75.0,2,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Soil Temperature Units,10549,4.5,9240,6.0,9587,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,9412,0ae23dce-a6f9-4729-8097-dc48f9253773,61ef6c2e-4b10-4cad-b351-a4fff5afa801,3d95ceba-e129-43da-a7a7-dc31b119a166,59f19149-08e8-4547-be1f-3e0bf80f75cf,8.0,390,199,2,43edf03f-a0b7-4679-b2cd-6597c1241cfc,4.0,10300,181139ce-59de-4037-9367-bd3cfe7ea74f,5.0,6.0,583,5444,7.0,11388,1.0,2.0,3.0,4.0,5.0,1426,6.0,Yield Goal Units,7.0,8.0,11,6.0,4.0,4.0,Distance Units,3.0,6.0,6.0,3.0,4.0,0.2,45.0,1,7,13,9565,6058,10005,9552,12039,591,12457,9751,1158,11150,12273,11394,12558,2,4.0,5.0,400,5.0,0.01,4.5,6.0,Seed Treatment Units,Seed Treatment Units,Seed Treatment Units,1,Seed Treatment Units\n";
	private static final String CSV_ARRAY_WITH_HEADERS = CSV_HEADERS + "\n" + CSV_ARRAY_NO_HEADERS;

	private static final String JSON_OBJECT_RESOURCE = "json-csv-object.json";
	private static final String CSV_OBJECT_NO_HEADERS = "3.0,730,510,9.0,13,731,2.0,5,1.0,13,4.0,892,3.0,120,1,1,John,Time Units,2.0,6.0,Speed Units,1.0,5.0,6.0,5.0,4.0,3.0,3,4,1,2,13,Helena,6.0,591,250.0,11228,11582,4692,11379,10528,9329,11502,56,4.0,2017-09-08T16:20:04Z,Air Temperature Units,Spray Pressure Units,7,Additive Units,Additive Units,Additive Units,Additive Units,Additive Units,b788b9c4-1fe6-4007-9889-e6445ee3727b,Additive Units,180,511,163,277,366,7,447,277,red,REI Units,90.0,1060,5.0,8,Post-App Rainfall Units,1060,1060,5,4,6,1,lake,3,2,1581,20,13,936,1701,977,5,280,5.0,Total App. Units,64821,1,13,PHI/WHP Units,Nozzle Spacing Units,1,Plant Back Units,2.0,5559354a-0b5d-4cdc-9cd0-a985f446c62d,1.0,4.0,3.0,6.0,5.0,Plant Spacing Units,18.0,9591,5266,12282,5444,1,12557,1,5345,5423,5,1,2,3,10887,1,17,Growth Regulator Units,Growth Regulator Units,spraying co.,Growth Regulator Units,Growth Regulator Units,68.0,2,24.0,Nitrogen Stabilizer Units,40,1,13,85.14494974216365,1,4.0,4.5,2.0,4.0,712,2,5,5,4,3,Herbicide Units,2,6,Herbicide Units,Herbicide Units,Herbicide Units,1,Herbicide Units,Herbicide Units,Herbicide Units,Herbicide Units,1.0,3,310,349,349,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,Fungicide Units,1,Fungicide Units,Fungicide Units,1.0,68.0,4.0,Wind Speed Units,8.0,6.0,7.0,4.0,5.0,6058,6023,2016-12-19T18:36:21Z,9176,9164,2,Row Spacing Units,here are some comments,Fertilizer Rate Units,2.0,Fertilizer Rate Units,3.0,Fertilizer Rate Units,Fertilizer Rate Units,1.0,Fertilizer Rate Units,Fertilizer Rate Units,7642,8478,10070,7496,9789,12.0,10542,2,8740,10061,8,6.0,6.0,9562,4.0,7642,7496,7589,8390,1,9359,45.0,11.0,9571,10.0,10003,6.0,11.0,4.0,3.0,3,2,1,41.0,7,5,13.0,6,45.0,5,45.0,4,499,362,1,61,2,Custom Fertilizer Units,7,Custom Fertilizer Units,8,Custom Fertilizer Units,Custom Fertilizer Units,Custom Fertilizer Units,3,Custom Fertilizer Units,4,5,6,32000.0,2,4,152,1.0,123456,sprayer,6,4.0,1,1.0,2.0,3,3.0,2,5,4,6,16.0,Load Size Units,18,2,1,8,7,5,4,3,6,5,120.0,1,705,Austin,21.0,10.0,Implement Width Units,25.0,12.0,30.0,10.0,123456,2,394,45,Seeding Units,1,2,3.0,4.0,1.0,2.0,Band Width Units,4.0,1,4.0,7.0,4.0,Height Units,11,2.0,1,4.0,4.0,3.0,5fc63839-d4ce-4f34-8fe6-f58640ebca09,12.0,Tillage Depth Units,10.0,Pre-App Rainfall Units,354,10086,1,Delta T Units,8.0,11174,9864,Frequency Units,9651,5.0,28,2,Planting Depth Units,5266,4.0,9195,6.0,5423,4.0,5345,6.0,Insecticide Units,Insecticide Units,11581,75.0,2,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Insecticide Units,Soil Temperature Units,10549,4.5,9240,6.0,9587,8c3fd22a-bd4c-448c-a3c4-329164e5e4c8,9412,0ae23dce-a6f9-4729-8097-dc48f9253773,61ef6c2e-4b10-4cad-b351-a4fff5afa801,3d95ceba-e129-43da-a7a7-dc31b119a166,59f19149-08e8-4547-be1f-3e0bf80f75cf,8.0,390,199,2,43edf03f-a0b7-4679-b2cd-6597c1241cfc,4.0,10300,181139ce-59de-4037-9367-bd3cfe7ea74f,5.0,6.0,583,5444,7.0,11388,1.0,2.0,3.0,4.0,5.0,1426,6.0,Yield Goal Units,7.0,8.0,11,6.0,4.0,4.0,Distance Units,3.0,6.0,6.0,3.0,4.0,0.2,45.0,1,7,13,9565,6058,10005,9552,12039,591,12457,9751,1158,11150,12273,11394,12558,2,4.0,5.0,400,5.0,0.01,4.5,6.0,Seed Treatment Units,Seed Treatment Units,Seed Treatment Units,1,Seed Treatment Units\n";
	private static final String CSV_OBJECT_WITH_HEADERS = CSV_HEADERS + "\n" + CSV_OBJECT_NO_HEADERS;

	public JsonToFixedCSVServiceTest()
	{
		super("Json to fixed CSV service test");
	}

//	@Test
//	public void testServiceWithArray()
//	{
//		AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(JSON_ARRAY);
//	}

	@Test
	public void testServiceWithObjectWithAutoDetectWithHeaders() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		JsonToFixedCSVService service = getService(true, true, null);

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_WITH_HEADERS, message.getContent());
	}

	@Test
	public void testServiceWithObjectWithAutoDetectNoHeaders() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		JsonToFixedCSVService service = getService(true, false, null);

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_NO_HEADERS, message.getContent());
	}

	@Test
	public void testServiceWithObjectNoAutoDetectWithHeaders() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		JsonToFixedCSVService service = getService(false, true, getHeaders(CSV_HEADERS));

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_WITH_HEADERS, message.getContent());
	}

	@Test
	public void testServiceWithObjectNoAutoDetectNoHeaders() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT_RESOURCE);
		JsonToFixedCSVService service = getService(false, false, getHeaders(CSV_HEADERS));

		execute(service, message);

		Assert.assertEquals(CSV_OBJECT_NO_HEADERS, message.getContent());
	}

	private List<String> getHeaders(String headers)
	{
		String[] h1 = headers.split(",");
		List<String> h2 = new ArrayList();
		for (String header : h1)
		{
			h2.add(header);
		}
		return h2;
	}

	private AdaptrisMessage getMessage(String resource) throws IOException
	{
		return AdaptrisMessageFactory.getDefaultInstance().newMessage(IOUtils.toString(getClass().getResourceAsStream(resource), "UTF-8"));
	}

	private JsonToFixedCSVService getService(boolean autoDetect, boolean showHeaders, List<String> headers)
	{
		JsonToFixedCSVService service = (JsonToFixedCSVService)retrieveObjectForSampleConfig();
		service.setAutoDetectHeaders(autoDetect);
		service.setShowHeaders(showHeaders);
		if (!autoDetect)
		{
			service.setCsvHeaders(headers);
		}
		return service;
	}

	@Override
	protected Object retrieveObjectForSampleConfig()
	{
		return new JsonToFixedCSVService();
	}
}
