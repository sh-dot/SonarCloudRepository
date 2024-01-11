// <copyright file="MachineRepository.cs" company="Komatsu">
//     Copyright (c) Microsoft. All rights reserved.
// </copyright>

namespace SNMP.DataAccess.Persistence.Repositories
{
    using System;
    using System.Collections.Generic;
    using System.Dynamic;
    using System.Globalization;
    using System.Linq;
    using System.Net.Http;
    using System.Net.Http.Headers;
    using System.Text;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Data.SqlClient;
    using Microsoft.EntityFrameworkCore;
    using Newtonsoft.Json;
    using SNMP.BusinessModels;
    using SNMP.BusinessModels.GridModel;
    using SNMP.BusinessModels.MachineDetail;
    using SNMP.BusinessServices;
    using SNMP.Core.Utility;
    using SNMP.DataAccess.Core.Cosmos;
    using SNMP.DataAccess.Core.Repositories;
    using SNMP.DataAccess.Extension;
    using SNMP.DataModels;
    using SNMP.DataModels.CosmosModel;
    using SNMP.DataModels.Models;
    using SNMP.DataModels.Models.LookupValue;
    using SNMP.DataModels.Models.MachineGridAlerts;
    using static SNMP.Core.Utility.Constants;

    /// <summary>
    /// Machine Repository.
    /// </summary>
    public class MachineRepository : IMachineRepository
    {
        /// <summary>
        /// The SNMP context..
        /// </summary>
        private readonly SnmpContext snmpContext;

        /// <summary>
        /// Defines the cosmos DB credentials.....
        /// </summary>
        private readonly SnmpCosmosContext cosmosdbcredentials;

        /// <summary>
        /// Defines the containerConfig.
        /// </summary>
        private readonly ContainerConfig containerConfig;

        /// <summary>
        /// Defines the branchSystemDataConfig.
        /// </summary>
        private readonly BranchSystemDataConfig branchSystemDataConfig;

        /// <summary>
        /// Defines the cosmos DB.....
        /// </summary>
        private CosmosConfigurationRepository cosmosdb;

        /// <summary>
        /// Defines the maskProperties.
        /// </summary>
        private IMaskPropertyRepository maskProperties;

        /// <summary>
        /// Initializes a new instance of the <see cref="MachineRepository"/> class.
        /// </summary>
        /// <param name="maskProperty">The maskProperty<see cref="IMaskPropertyRepository"/>.</param>
        /// <param name="cosmosContext">The cosmosContext<see cref="SnmpCosmosContext"/>.</param>
        /// <param name="context">The context<see cref="SnmpContext"/>.</param>
        /// <param name="branchSystemDataConfig">The branchSystemDataConfig<see cref="BranchSystemDataConfig"/>.</param>
        /// <param name="container">The container<see cref="ContainerConfig"/>.</param>
        public MachineRepository(IMaskPropertyRepository maskProperty, SnmpCosmosContext cosmosContext, SnmpContext context, BranchSystemDataConfig branchSystemDataConfig, ContainerConfig container)
        {
            this.branchSystemDataConfig = branchSystemDataConfig;
            this.cosmosdbcredentials = cosmosContext;
            this.containerConfig = container;
            this.snmpContext = context;
            this.maskProperties = maskProperty;
        }

        /// <summary>
        /// Gets all.
        /// </summary>
        /// <param name="pageIndex">Index of the page.</param>
        /// <param name="pageSize">Size of the page.</param>
        /// <returns>IEnumerable List of Machine.</returns>
        public async Task<IEnumerable<Machine>> GetAllAsync(int pageIndex, int pageSize)
        {
            var lst = await this.snmpContext.Machines
                .OrderBy(c => c.MachineName)
                .Skip((pageIndex - 1) * pageSize)
                .Take(pageSize)
                .ToListAsync().ConfigureAwait(false);
            return lst;
        }

        /// <summary>
        /// Gets the machine details.
        /// </summary>
        /// <returns>IEnumerable List of Machine Details.</returns>
        public async Task<IList<MachineDetails>> GetMachineDetailsAsync()
        {
            return await this.snmpContext.MachineDetails.ToListAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the data asynchronous.
        /// </summary>
        /// <param name="criteria">The criteria.</param>
        /// <returns>IEnumerable List of Machine Details.</returns>
        public async Task<IList<MachineDetails>> GetDataAsync(GridResultCriteria criteria)
        {
            _ = criteria ?? throw new ArgumentNullException(nameof(criteria));
            NumberFormatInfo customNumFormat;
            customNumFormat = (NumberFormatInfo)CultureInfo.InvariantCulture.NumberFormat.Clone();
            customNumFormat.NumberGroupSeparator = ",";
            customNumFormat.NumberDecimalSeparator = ".";
            customNumFormat.NumberDecimalDigits = 0;

            List<MachineDetails> results = new List<MachineDetails>();

            int pageNo = Convert.ToInt32(criteria.TotalRecords) / 4000;
            int remainder = Convert.ToInt32(criteria.TotalRecords) % 4000;
            pageNo = remainder > 0 ? pageNo + 1 : pageNo;
            for (int i = 0; i < pageNo; i++)
            {
                var postData = new
                {
                    SubScriptionKey = criteria.SubScriptionKey,
                    Email = criteria.EmailAddress,
                    View = criteria.View,
                    Page = i + 1,
                    PageSize = 4000,
                    BulkExport = true,
                    resolvedType = "Resolved",
                    Application = criteria.Application,
                    Filters = criteria.GlobalFilter,
                    Sort = criteria.Sort,
                    TargetProxyUser = criteria.TargetProxyUser
                };
                Uri uri = UrlValidatorExtension.ToUri(criteria.ApiEndPoint);
                using (var client = new HttpClient())
                {
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                    client.DefaultRequestHeaders.Add("access-token", criteria.AccessToken.Replace("bearer ", string.Empty, StringComparison.OrdinalIgnoreCase));
                    client.DefaultRequestHeaders.Add("authorization", criteria.AccessToken.Replace("bearer ", "Bearer ", StringComparison.InvariantCulture));
                    client.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", criteria.SubScriptionKey);
                    client.DefaultRequestHeaders.Add("Cache-Control", "no-cache");
                    StringContent content = new StringContent(JsonConvert.SerializeObject(postData), Encoding.UTF8, "application/json");
                    HttpResponseMessage response = await client.PostAsync(uri, content).ConfigureAwait(false);
                    if (response.IsSuccessStatusCode)
                    {
                        string data = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                        var models = JsonConvert.DeserializeObject<dynamic>(data);
                        var result = models.result.machineDetails;
                        foreach (var dynamic in result)
                        {
                            MachineDetails machineDetails = new MachineDetails();
                            machineDetails.MachineType = dynamic.machineType;
                            machineDetails.CustomerName = dynamic.customerName;
                            machineDetails.FullModel = dynamic.fullModel;
                            machineDetails.SerialNumber = dynamic.serialNumber;

                            //// machineDetails.Latitude = dynamic.latitude == null ? 0 : Convert.ToDouble(dynamic.latitude);
                            machineDetails.SMR = dynamic.smr == null || dynamic.smr == 0 ? string.Empty : string.Format("{0:#,##0.##}", dynamic.smr);
                            machineDetails.WarrantyExpiration = dynamic.warrantyExpiration;
                            machineDetails.FirstInDirt = dynamic.firstInDirt;
                            machineDetails.DeliveryDate = dynamic.deliveryDate;
                            machineDetails.MachineStatus = dynamic.machineStatus;
                            machineDetails.OrgName = dynamic.orgName;
                            machineDetails.Area1 = dynamic.area1;
                            machineDetails.Area2 = dynamic.area2;
                            machineDetails.LocationDistributor = dynamic.locationDistributor;
                            machineDetails.LastCommunicationDate = this.CheckGpsGreaterSmr(dynamic.gpsUpTime, dynamic.smrUpTime);
                            machineDetails.TerritoryOwner = this.SetTerritoryOwner(dynamic.territory);
                            machineDetails.TerritoryGroupOwner = this.SetTerritoryGroupOwner(dynamic.territory);
                            machineDetails.PSSRName = (machineDetails.TerritoryOwner == TerritoryName.GeneralValue) ? string.Empty : dynamic.pssrName;
                            machineDetails.Branch = dynamic.branch;

                            //// if (dynamic.firstinDirt != null)
                            //// {
                            ////     machineDetails.FID = dynamic.firstinDirt;
                            //// }

                            //// if (dynamic.finalDelivery != null)
                            //// {
                            ////    machineDetails.FD = dynamic.finalDelivery;
                            //// }

                            //// machineDetails.PINNumber = dynamic.pin;
                            //// machineDetails.MachineStatus = dynamic.machineStatus;
                            results.Add(machineDetails);
                        }
                    }
                }
            }

            return results;
        }

        /// <summary>
        /// Gets GPS or SMR date.
        /// </summary>
        /// <param name="gpsUpTime">The GPS date.</param>
        /// <param name="smrUpTime">The SMR date. identifier.</param>
        /// <returns>The name <see cref="string"/>.</returns>
        public dynamic CheckGpsGreaterSmr(dynamic gpsUpTime, dynamic smrUpTime)
        {
            try
            {
                if (gpsUpTime != null && smrUpTime != null)
                {
                    dynamic gpsDate = gpsUpTime;
                    dynamic smrDate = smrUpTime;
                    if (gpsUpTime.GetType().Name == "String")
                    {
                        gpsDate = this.CheckDateValidityFromString(gpsUpTime, Constants.DateMappingFormat.Date);
                    }

                    if (smrUpTime.GetType().Name == "String")
                    {
                        smrDate = this.CheckDateValidityFromString(smrUpTime, Constants.DateMappingFormat.Date);
                    }

                    if (gpsDate > smrDate)
                    {
                        return gpsUpTime;
                    }
                    else if (gpsDate == null)
                    {
                        return this.CheckAndSetValidDateFromDynamic(smrUpTime);
                    }
                    else if (smrDate == null)
                    {
                        return this.CheckAndSetValidDateFromDynamic(gpsUpTime);
                    }
                    else
                    {
                        return this.CheckAndSetValidDateFromDynamic(smrUpTime);
                    }
                }
                else if (gpsUpTime != null)
                {
                    return gpsUpTime;
                }
                else
                {
                    return this.CheckAndSetValidDateFromDynamic(smrUpTime);
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// Gets the Date information.
        /// </summary>
        /// <param name="dateString">The territory identifier.</param>
        /// <returns>The name <see cref="string"/>.</returns>
        public dynamic CheckAndSetValidDateFromDynamic(dynamic dateString)
        {
            try
            {
                if (dateString != null)
                {
                    return dateString;
                }
                else
                {
                    return null;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// Gets the territory name.
        /// </summary>
        /// <param name="ch">The territory identifier.</param>
        /// <param name="territoryName">The territory .</param>
        /// <param name="territoryOwner">The territory identifiers.</param>
        /// <returns>The name <see cref="string"/>.</returns>
        public string CheckTerritoryAndTerritoryGroupChar(string ch, dynamic territoryName, dynamic territoryOwner)
        {
            if ((territoryName != null) &&
              (territoryOwner != null))
            {
                return ch;
            }

            return string.Empty;
        }

        /// <summary>
        /// Gets the territory group name.
        /// </summary>
        /// <param name="text">The territory identifier.</param>
        /// <returns>The name <see cref="string"/>.</returns>
        public string CheckNullAndEmpty(dynamic text)
        {
            if (text != null)
            {
                return text;
            }
            else
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Gets the territory information.
        /// </summary>
        /// <param name="territory">The territory identifier.</param>
        /// <returns>The name <see cref="string"/>.</returns>
        public string SetTerritoryOwner(dynamic territory)
        {
            if (territory == null || territory.Type == Newtonsoft.Json.Linq.JTokenType.Null)
            {                
                return string.Empty;
            }
            else
            {
                string terrName = string.Empty;
                foreach (var item in territory)
                {
                    terrName = item.catId == 3 ? !this.CheckGeneralTerritoryAndGroup(this.CheckNullAndEmpty(item.territoryName)) ? this.CheckNullAndEmpty(item.territoryName) + this.CheckTerritoryAndTerritoryGroupChar("-", item.territoryName, item.territoryOwner) + this.CheckNullAndEmpty(item.territoryOwner) : TerritoryName.GeneralValue : terrName;
                }

                if (!string.IsNullOrEmpty(terrName))
                {
                    return terrName;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Gets the territory group name.
        /// </summary>
        /// <param name="territory">The territory identifier.</param>
        /// <returns>The name <see cref="string"/>.</returns>
        public string SetTerritoryGroupOwner(dynamic territory)
        {
            if (territory == null || territory.Type == Newtonsoft.Json.Linq.JTokenType.Null)
            {
                return string.Empty;
            }
            else
            {
                string groupName = string.Empty;
                foreach (var item in territory)
                {
                    groupName = item.catId == 3 ? !this.CheckGeneralTerritoryAndGroup(this.CheckNullAndEmpty(item.groupName)) ? this.CheckNullAndEmpty(item.groupName) + this.CheckTerritoryAndTerritoryGroupChar("-", item.groupName, item.groupOwner) + this.CheckNullAndEmpty(item.groupOwner) : TerritoryName.GeneralValue : groupName;
                }

                if (!string.IsNullOrEmpty(groupName))
                {
                    return groupName;
                }
            }

            return string.Empty;
        }

        /// <summary>
        /// Gets general territory and group.
        /// </summary>
        /// <param name="name">The territory.</param>
        /// <returns>The <see cref="bool"/>.</returns>
        public bool CheckGeneralTerritoryAndGroup(string name)
        {
            if (name.Trim().ToLower(CultureInfo.CurrentCulture) == TerritoryName.General.ToLower(CultureInfo.CurrentCulture))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        /// <summary>
        /// Gets the machine alerts .
        /// </summary>
        /// <param name="alertObj">The alert object.</param>
        /// <returns>The <see cref="int"/>.</returns>
        public int CheckAndGetAlert(dynamic alertObj)
        {
            if (alertObj != null)
            {
                return alertObj.Count;
            }
            else
            {
                return 0;
            }
        }

        /// <summary>
        /// Gets the machine alert details asynchronous.
        /// </summary>
        /// <param name="criteria">The criteria<see cref="AlertHistoryCriteria"/>.</param>
        /// <returns>Machine Alert Details.</returns>
        public async Task<IList<AlertsHistory>> GetMachineCrossBorderAlertDetailsAsync(MachineHistoryGridCriteria criteria)
        {
            List<AlertsHistory> result = new List<AlertsHistory>();

            QueryDefinition query = new QueryDefinition("SELECT c.fullModel, c.serial, c.alertType, bd.kgpOrgId, bd.alertData FROM c JOIN bd IN c.branchData Where UPPER(bd.status)='ACTIVE' AND bd.triggerOrgId = @orgId AND c.schemaName IN (@AlertCrossBoderInSchemaName, @AlertCrossBoderOutSchemaName) AND bd.alertType in ('Cross Border In','Cross Border Out') AND c.branchName = @branchname AND c.trunkModel = @fullModel AND c.serial = @serial")
            .WithParameter("@orgId", criteria.KGPOrgID)
            .WithParameter("@fullModel", criteria.FullModel).WithParameter("@serial", criteria.Serial)
            .WithParameter("@branchname", this.branchSystemDataConfig.AlertBranchName)
            .WithParameter("@AlertCrossBoderInSchemaName", this.branchSystemDataConfig.AlertCrossBorderInSchemaName)
            .WithParameter("@AlertCrossBoderOutSchemaName", this.branchSystemDataConfig.AlertCrossBorderOutSchemaName);
            this.cosmosdbcredentials.CollectionId = this.containerConfig.BranchSystemData;
            this.cosmosdb = new CosmosConfigurationRepository(this.cosmosdbcredentials);
            FeedIterator<AlertsHistory> data = this.cosmosdb.ExecuteQuery<AlertsHistory>(query);
            while (data.HasMoreResults)
            {
                var response = await data.ReadNextAsync().ConfigureAwait(false);
                result.AddRange(response);
            }

            return result;
        }

        /// <summary>
        /// Gets the machine detail information.
        /// </summary>
        /// <param name="criteria">The criteria identifier.</param>
        /// <returns>The <see cref="Task{MachineInfoModel}"/>.</returns>
        public async Task<MachineInfoModel> GetMachineDetailedInformations(MachineHistoryGridCriteria criteria)
        {
            QueryDefinition query;
            List<CustomerAddressModel> customers = new List<CustomerAddressModel>();
            MachineInfoModel machineInfo = new MachineInfoModel();

            if (criteria.View.ToLower(CultureInfo.CurrentCulture) == Constants.ViewType.Distributor.ToLower(CultureInfo.CurrentCulture))
            {
                List<string> claimList = KConstants.GetClaims(KConstants.DistributorView);
                var orgIdList = await this.maskProperties.GetOrgIdListWithPermissionDistributerView(criteria, claimList).ConfigureAwait(false);
                query = new QueryDefinition("SELECT * FROM c where c.trModel = @fullModel and c.sN = @serial and c.kgpOrgId = @kgpOrgID")
                    .WithParameter("@fullModel", criteria.FullModel).WithParameter("@serial", criteria.Serial).WithParameter("@kgpOrgID", criteria.KGPOrgID);
                this.cosmosdbcredentials.CollectionId = this.containerConfig.DistributorMachineMaster;
                this.cosmosdb = new CosmosConfigurationRepository(this.cosmosdbcredentials);
                FeedIterator<MachineInfoCosmosModel> feedIterator = this.cosmosdb.ExecuteQuery<MachineInfoCosmosModel>(query);
                List<MachineInfoCosmosModel> machineInfoCosmos = new List<MachineInfoCosmosModel>();
                while (feedIterator.HasMoreResults)
                {
                    var response = await feedIterator.ReadNextAsync().ConfigureAwait(false);
                    machineInfoCosmos.AddRange(response);
                }

                foreach (var item in machineInfoCosmos)
                {
                    machineInfo.MachineType = this.SetMachineType(item.MachineType, item.OEM);
                    machineInfo.FullModel = item.TrModel;
                    machineInfo.Serial = item.Serial;
                    machineInfo.PinNumber = item.Pin;
                    machineInfo.MachineCategoryName = item.ProductType;
                    machineInfo.Distributor = item.KgpOrgName;
                    machineInfo.LocationDbName = item.LocationDbName;
                    machineInfo.Area1 = item.Area1;
                    machineInfo.Area2 = item.Area2;
                    machineInfo.PssrName = item.Pic != null ? item.Pic.FirstOrDefault(x => x.CatId == "3")?.Name : string.Empty;
                    machineInfo.CustomerName = item.CustomerNameML != null ? item.CustomerNameML.FirstOrDefault().Name : string.Empty;
                    machineInfo.TerritoryGroupName = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "3")?.GroupName : string.Empty;
                    machineInfo.TerritoryGroupOwner = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "3")?.GroupOwner : string.Empty;
                    machineInfo.TerritoryName = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "3")?.TerritoryName : string.Empty;
                    machineInfo.TerritoryOwner = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "3")?.TerritoryOwner : string.Empty;
                    machineInfo.FinalDeliveryDate = this.CheckDateValidityFromString(item.FinalDeliveryDate, Constants.DateMappingFormat.String);
                    machineInfo.Smr = item.Telemetry != null && item.Telemetry.Count > 0 ? decimal.Parse(item.Telemetry.FirstOrDefault().SMR ?? "0.0") : decimal.Parse("0.0");
                    machineInfo.SmrUpTime = item.Telemetry != null && item.Telemetry.Count > 0 ? this.CheckGpsGreaterSmr(item.Telemetry.FirstOrDefault().GpsUpTime, item.Telemetry.FirstOrDefault().SmrUpTime) : string.Empty; /*latestDate*/
                    machineInfo.SmrSource = item.Telemetry != null && item.Telemetry.Count > 0 ? item.Telemetry.FirstOrDefault().SmrSource : string.Empty; /*latestSMRSource*/
                    machineInfo.Latitude = item.Telemetry != null && item.Telemetry.Count > 0 ? double.Parse(item.Telemetry.FirstOrDefault().Lat ?? "0") : double.Parse("0");
                    machineInfo.Longitude = item.Telemetry != null && item.Telemetry.Count > 0 ? double.Parse(item.Telemetry.FirstOrDefault().Lon ?? "0") : double.Parse("0");
                    machineInfo.EtlMachineId = item.Telemetry != null && item.Telemetry.Count > 0 ? ValidateMachineID(item.Telemetry.FirstOrDefault().ETLMachineId) ?? string.Empty : string.Empty;
                    machineInfo.GpsUpTime = item.Telemetry != null && item.Telemetry.Count > 0 ? this.CheckDateValidityFromString(item.Telemetry.FirstOrDefault().GpsUpTime, Constants.DateMappingFormat.String) : string.Empty;
                    machineInfo.BuildLocation = item.BuildLocationDMM;
                    machineInfo.BuildDate = string.IsNullOrEmpty(item.BuildDateDMM) ? null : this.CheckDateValidityFromString(item.BuildDateDMM, Constants.DateMappingFormat.String);
                    machineInfo.EngineModel = item.EngineModel;

                    machineInfo.SaleType = item.SalesType;
                    machineInfo.InvoicedToDistributorDate = this.CheckDateValidityFromString(item.InvoicedToDistributorDateDMM, Constants.DateMappingFormat.String);
                    machineInfo.FidDate = string.IsNullOrEmpty(item.FirstInDirtDateDMM) ? null : this.CheckDateValidityFromString(item.FirstInDirtDateDMM, Constants.DateMappingFormat.String);

                    machineInfo.CrossBorderIn = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Cross Border In".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.CrossBorderOut = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Cross Border Out".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.ContractTermination = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Contract Termination".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.MaintenanceAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "PM Job".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.ErrorCodeAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Abnormality".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.General = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "General".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.EngineOvAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Engine OV By Fuel".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.UndercarriageAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "UC Replacement".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.KomatsuCareAlert = 0;
                    ////machineInfo.KomatsuCareAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Komatsu Care".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo = MaskProperties(orgIdList, item.KgpOrgId, machineInfo);
                }

                ////long kgpOrgId = Decimal.ToInt64(machineInfoCosmos.FirstOrDefault().KgpOrgId);
                var kgpOrgId = (long?)0;
                var custAccNo = string.Empty;
                ////long? kgpOrgId = machineInfoCosmos.FirstOrDefault().KgpOrgId.GetValueOrNull<long>();
                if (machineInfoCosmos.Count > 0 && !string.IsNullOrEmpty(machineInfoCosmos.FirstOrDefault().KgpOrgId))
                {
                    kgpOrgId = Convert.ToInt64(machineInfoCosmos.FirstOrDefault().KgpOrgId);
                }

                ////var kgpOrgId = (machineInfoCosmos.Count <= 0 && string.IsNullOrEmpty(machineInfoCosmos.FirstOrDefault().KgpOrgId)) ? (long?)0 : Convert.ToInt64(machineInfoCosmos.FirstOrDefault().KgpOrgId);

                if (machineInfoCosmos.Count > 0 && machineInfoCosmos.FirstOrDefault().CustomerNameML.Count > 0 && !string.IsNullOrEmpty(machineInfoCosmos.FirstOrDefault().CustomerNameML.FirstOrDefault().AccNo))
                {
                    custAccNo = machineInfoCosmos.FirstOrDefault().CustomerNameML.FirstOrDefault().AccNo;
                }

                if (kgpOrgId != 0 && !string.IsNullOrEmpty(custAccNo))
                {
                    query = new QueryDefinition("SELECT c.billingaddress1, c.billingaddress2, c.billingcity, c.billingstate, c.billingzip, c.billingcountry  FROM c Where c.orgid =@OrgId and c.accno = @CustacNo")
                            .WithParameter("@OrgId", kgpOrgId).WithParameter("@CustacNo", custAccNo);
                    this.cosmosdbcredentials.CollectionId = this.containerConfig.DistributorCustomerMaster;
                    this.cosmosdb = new CosmosConfigurationRepository(this.cosmosdbcredentials);
                    FeedIterator<CustomerAddressModel> feedIterators = this.cosmosdb.ExecuteQuery<CustomerAddressModel>(query);
                    while (feedIterators.HasMoreResults)
                    {
                        var response = await feedIterators.ReadNextAsync().ConfigureAwait(false);
                        customers.AddRange(response);
                    }

                    if (customers.Count > 0 && !(customers.FirstOrDefault() is null) && !string.IsNullOrEmpty(customers.FirstOrDefault().BillingAddress1))
                    {
                        machineInfo.CustomerLocation = string.Concat(customers.FirstOrDefault().BillingAddress1, " ", customers.FirstOrDefault().BillingAddress2, ", ", customers.FirstOrDefault().BillingCity, ", ", customers.FirstOrDefault().BillingState, ", ", customers.FirstOrDefault().BillingZip, ", ", customers.FirstOrDefault().BillingCountry);
                    }
                }
            }
            else
            {
                List<string> claimList = KConstants.GetClaims(KConstants.TrunkView);
                var orgIdList = await this.maskProperties.GetOrgIdListWithPermissionTrunkSerial(criteria, claimList).ConfigureAwait(false);
                query = new QueryDefinition("SELECT * FROM c where c.salesModel = @fullModel and c.sN = @serial")
                .WithParameter("@fullModel", criteria.FullModel).WithParameter("@serial", criteria.Serial);
                this.cosmosdbcredentials.CollectionId = this.containerConfig.TrunkSerial;
                this.cosmosdb = new CosmosConfigurationRepository(this.cosmosdbcredentials);
                FeedIterator<MachineInfoCosmosModel> feedIterator = this.cosmosdb.ExecuteQuery<MachineInfoCosmosModel>(query);
                List<MachineInfoCosmosModel> result = new List<MachineInfoCosmosModel>();
                while (feedIterator.HasMoreResults)
                {
                    var response = await feedIterator.ReadNextAsync().ConfigureAwait(false);
                    result.AddRange(response);
                }

                foreach (var item in result)
                {
                    machineInfo.MachineType = this.SetMachineType(item.MachineType, item.OEM);
                    machineInfo.FullModel = item.SalesModel;
                    machineInfo.Serial = item.Serial;
                    machineInfo.PinNumber = item.PINNumber;
                    machineInfo.MachineCategoryName = item.ProductType;
                    machineInfo.Distributor = item.OrganizationName;
                    machineInfo.LocationDbName = item.LocationDbName;
                    machineInfo.Area1 = item.Area1;
                    machineInfo.Area2 = item.Area2;
                    //// PSSR
                    machineInfo.PssrName = item.Pic != null ? item.Pic.FirstOrDefault(x => x.CatId == "3")?.Name : string.Empty;
                    //// CustomerName
                    machineInfo.CustomerName = item.CustomerName;
                    machineInfo.CustomerLocation = item.CustomerAddress;
                    machineInfo.TerritoryGroupName = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "2")?.GroupName : string.Empty;
                    machineInfo.TerritoryGroupOwner = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "2")?.GroupOwner : string.Empty;
                    machineInfo.TerritoryName = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "2")?.TerritoryName : string.Empty;
                    machineInfo.TerritoryOwner = item.Territory != null && item.Territory.Count > 0 && item.Territory.FirstOrDefault() != null ? item.Territory.FirstOrDefault(x => x.CatId == "2")?.TerritoryOwner : string.Empty;
                    machineInfo.FinalDeliveryDate = this.CheckDateValidityFromString(item.FinalDeliveryDate, Constants.DateMappingFormat.String);
                    machineInfo.Smr = item.Telemetry != null && item.Telemetry.Count > 0 ? decimal.Parse(item.Telemetry.FirstOrDefault().SMR ?? "0.0") : decimal.Parse("0.0");
                    machineInfo.SmrUpTime = item.Telemetry != null && item.Telemetry.Count > 0 ? (this.CheckDateValidityFromString(item.Telemetry.FirstOrDefault().SmrUpTime, Constants.DateMappingFormat.String) != null ? this.CheckDateValidityFromString(item.Telemetry.FirstOrDefault().SmrUpTime, Constants.DateMappingFormat.String) : this.CheckDateValidityFromString(item.Telemetry.FirstOrDefault().GpsUpTime, Constants.DateMappingFormat.String)) : string.Empty; /*latestDate*/
                    machineInfo.SmrSource = item.Telemetry != null && item.Telemetry.Count > 0 ? item.Telemetry.FirstOrDefault().SmrSource : string.Empty; /*latestSMRSource*/
                    //// Map
                    machineInfo.Latitude = item.Telemetry != null && item.Telemetry.Count > 0 ? double.Parse(item.Telemetry.FirstOrDefault().Lat ?? "0") : double.Parse("0");
                    machineInfo.Longitude = item.Telemetry != null && item.Telemetry.Count > 0 ? double.Parse(item.Telemetry.FirstOrDefault().Lon ?? "0") : double.Parse("0");
                    machineInfo.EtlMachineId = item.Telemetry != null && item.Telemetry.Count > 0 ? ValidateMachineID(item.Telemetry.FirstOrDefault().ETLMachineId) ?? string.Empty : string.Empty;
                    machineInfo.GpsUpTime = item.Telemetry != null && item.Telemetry.Count > 0 ? this.CheckDateValidityFromString(item.Telemetry.FirstOrDefault().GpsUpTime, Constants.DateMappingFormat.String) : string.Empty;
                    machineInfo.BuildLocation = item.BuildLocation;
                    machineInfo.BuildDate = string.IsNullOrEmpty(item.BuildDate) ? null : this.CheckDateValidityFromString(item.BuildDate, Constants.DateMappingFormat.String);
                    machineInfo.EngineModel = item.EngineModel;
                    machineInfo.SaleType = item.SalesType;
                    machineInfo.InvoicedToDistributorDate = this.CheckDateValidityFromString(item.InvoicedToDistributorDate, Constants.DateMappingFormat.String);
                    machineInfo.FidDate = string.IsNullOrEmpty(item.FirstInDirtDate) ? null : this.CheckDateValidityFromString(item.FirstInDirtDate, Constants.DateMappingFormat.String);
                    machineInfo.CrossBorderIn = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Cross Border In".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.CrossBorderOut = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Cross Border Out".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.ContractTermination = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Contract Termination".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.MaintenanceAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "PM Job".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.ErrorCodeAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Abnormality".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.General = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "General".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.EngineOvAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Engine OV By Fuel".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.UndercarriageAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "UC Replacement".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo.KomatsuCareAlert = 0;
                    ////machineInfo.KomatsuCareAlert = item.MachineAlerts != null && item.MachineAlerts.Count > 0 ? this.CheckAndGetAlert(item.MachineAlerts.FirstOrDefault(x => x.AlertType.Trim().ToLower(CultureInfo.CurrentCulture) == "Komatsu Care".ToLower(CultureInfo.CurrentCulture))) : 0;
                    machineInfo = MaskProperties(orgIdList, item.KgpOrgId, machineInfo);
                }
            }

            return machineInfo;
        }

        /// <summary>
        /// Gets valid date.
        /// </summary>
        /// <param name="date">The date.</param>
        /// <param name="dateFormat">The date format.</param>
        /// <returns>Get valid date.</returns>
        public dynamic CheckDateValidityFromString(string date, string dateFormat)
        {
            try
            {
                DateTime dt = DateTime.Parse(date);
                if (dateFormat == Constants.DateMappingFormat.Date)
                {
                    return dt;
                }
                else if (dateFormat == Constants.DateMappingFormat.String)
                {
                    return date;
                }
                else
                {
                    return date;
                }
            }
            catch (Exception ex)
            {
                return null;
            }
        }

        /// <summary>
        /// Gets the machine upcoming service list.
        /// </summary>
        /// <param name="machineId">The machine identifier.</param>
        /// <returns>Get Machine Upcoming Service List.</returns>
        public async Task<IList<MachineUpcomingServiceDetails>> GetMachineUpcomingServiceListAsync(int machineId)
        {
            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@MachineId", machineId)
            };

            return await this.snmpContext.MachineUpcomingServiceDetails.MultipleResultsAsync($"Exec {Constants.StoreProcedures.GetMachineUpcomingService} @MachineId", sqlParameter.ToArray<object>()).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the machine part change list.
        /// </summary>
        /// <param name="machineId">The machine identifier.</param>
        /// <returns>Get Machine Part Change List.</returns>
        public async Task<IList<MachinePartChangeDetails>> GetMachinePartChangeListAsync(int machineId)
        {
            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@MachineId", machineId)
            };

            return await this.snmpContext.MachinePartChangeDetails.MultipleResultsAsync($"Exec {Constants.StoreProcedures.GetMachinePartChange} @MachineId", sqlParameter.ToArray<object>()).ConfigureAwait(false);
        }

        /// <summary>
        /// Gets the machine status.
        /// </summary>
        /// <returns>list of Code Lookup.</returns>
        public async Task<IList<CodeLookupMaster>> GetMachineStatusAsync()
        {
            return await this.snmpContext.CodeLookupMaster.Where(x => x.Type == Application.CodeTypeMachineStatus).ToListAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Updates the machine status.
        /// </summary>
        /// <param name="machineStatus">The machine status.</param>
        /// <returns>returns 1 is success.</returns>
        public async Task<int> AddMachineStatusAsync(MachineStatus machineStatus)
        {
            var val = 0;
            if (machineStatus != null)
            {
                var machineStatusCurrent = this.snmpContext.MachineStatus.Where(x => x.MachineId == machineStatus.MachineId && x.IsActive).FirstOrDefault();
                if (machineStatusCurrent != null)
                {
                    machineStatusCurrent.IsActive = false;
                    machineStatusCurrent.ModifiedBy = machineStatus?.CreatedBy;
                    machineStatusCurrent.ModifiedDate = DateTime.UtcNow;
                }

                machineStatus.CreatedDate = DateTime.UtcNow;
                await this.snmpContext.MachineStatus.AddAsync(machineStatus).ConfigureAwait(false);
                val = await this.snmpContext.SaveChangesAsync().ConfigureAwait(false);
            }

            return val;
        }

        /// <summary>
        /// Gets the machine plot chart list.
        /// </summary>
        /// <param name="machineId">The machine identifier.</param>
        /// <param name="filters">The filters.</param>
        /// <returns>The <see cref="Task{IList{MachinePlotChart}}"/>.</returns>
        public async Task<IList<MachinePlotChart>> GetMachinePlotChartListAsync(int machineId, string filters)
        {
            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@Machineid", machineId),
                new SqlParameter("@LevelAndGroupname", filters)
            };

            var executeStatement = $"Exec {Constants.StoreProcedures.GetMachinePlotChartDetails} @MachineId, @levelandgroupname";
            var result = await this.snmpContext.MachinePlotChart.MultipleResultsAsync(executeStatement, sqlParameter.ToArray<object>()).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Gets the machine warranty program list.
        /// </summary>
        /// <param name="machineId">The machine identifier.</param>
        /// <returns>The <see cref="Task{IList{MachineWarranty}}"/>.</returns>
        public async Task<IList<MachineWarranty>> GetMachineWarrantyProgramListAsync(int machineId)
        {
            if (machineId <= 0)
            {
                throw new ArgumentNullException(nameof(machineId));
            }

            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@Machineid", machineId),
            };

            var executeStatement = $"Exec {Constants.StoreProcedures.GetMachineWarrantyProgram} @MachineId";
            var result = await this.snmpContext.MachineWarranty.MultipleResultsAsync(executeStatement, sqlParameter.ToArray<object>()).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Inserts the machine segmentation status.
        /// </summary>
        /// <param name="machineId">The machine identifier.</param>
        /// <param name="important">if set to <c>true</c> [is important].</param>
        /// <param name="createdBy">The created by.</param>
        /// <returns>The <see cref="Task{bool}"/>.</returns>
        public async Task<bool> UpsertMachineSegmentationStatusAsync(int machineId, bool important, string createdBy)
        {
            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@Machineid", machineId),
                new SqlParameter("@Isimportant", important),
                new SqlParameter("@CreatedBy", createdBy)
            };

            var executeStatement = $"Exec {Constants.StoreProcedures.UpsertMachineSegmentationStatus} @MachineId,@Isimportant, @CreatedBy";
            var result = await this.snmpContext.Database.ExecuteAsync(executeStatement, sqlParameter.ToArray<object>()).ConfigureAwait(false);
            return result > 0;
        }

        /// <summary>
        /// Get Machine Maintenance Ratio.
        /// </summary>
        /// <param name="machineId">The machine identifier.</param>
        /// <param name="intervalRange">interval range.</param>
        /// <returns>Return Machine maintenance ratio set.</returns>
        public async Task<IList<MaintenanceRatios>> GetMaintenanceRatioAsync(int machineId, int intervalRange)
        {
            if (machineId <= 0)
            {
                throw new ArgumentNullException(nameof(machineId));
            }

            if (intervalRange <= 0)
            {
                throw new ArgumentNullException(nameof(intervalRange));
            }

            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@Machineid", machineId),
                new SqlParameter("@IntervalRange", intervalRange)
            };

            var query = $"Exec {Constants.StoreProcedures.GetMaintenanceRatio} @MachineId,@IntervalRange";
            var result = await this.snmpContext.MaintenanceRatios.MultipleResultsAsync(query, sqlParameter.ToArray<object>()).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Return the data to export.
        /// </summary>
        /// <param name="criteria">criteria to get data.</param>
        /// <returns>list of machine alerts.</returns>
        public async Task<IList<MachineGridAlert>> GetDataForAlertExportAsync(GridResultCriteria criteria)
        {
            _ = criteria ?? throw new ArgumentNullException(nameof(criteria));

            var dynamicGridResultCriteria = RebuildGridResultCriteria(criteria);
            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@GlobalSearchtext", dynamicGridResultCriteria.GlobalSearchtext ?? (object)System.DBNull.Value),
                new SqlParameter("@IsGridFilterApplied", dynamicGridResultCriteria.IsGridFilterApplied ?? (object)System.DBNull.Value),
                new SqlParameter("@PssrIdList", dynamicGridResultCriteria.PssrIdList ?? (object)System.DBNull.Value),
                new SqlParameter("@CustomerIdList", dynamicGridResultCriteria.CustomerIdList ?? (object)System.DBNull.Value),
                new SqlParameter("@DistributorIdList", dynamicGridResultCriteria.DistributorIdList ?? (object)System.DBNull.Value),
                new SqlParameter("@BranchIdList", dynamicGridResultCriteria.BranchIdList ?? (object)System.DBNull.Value),
                new SqlParameter("@ModelList", dynamicGridResultCriteria.ModelList ?? (object)System.DBNull.Value),
                new SqlParameter("@SerialList", dynamicGridResultCriteria.SerialList ?? (object)System.DBNull.Value),
                new SqlParameter("@PINList", dynamicGridResultCriteria.PINList ?? (object)System.DBNull.Value),
                new SqlParameter("@MaxSMR", dynamicGridResultCriteria.MaxSMR ?? (object)System.DBNull.Value),
                new SqlParameter("@MinSMR", dynamicGridResultCriteria.MinSMR ?? (object)System.DBNull.Value),
                new SqlParameter("@MaxMachineHealth", dynamicGridResultCriteria?.MaxMachineHealth ?? (object)System.DBNull.Value),
                new SqlParameter("@MinMachineHealth", dynamicGridResultCriteria?.MinMachineHealth ?? (object)System.DBNull.Value),
                new SqlParameter("@FromWarrantyDate", dynamicGridResultCriteria.FromWarrantyDate ?? (object)System.DBNull.Value),
                new SqlParameter("@ToWarrantyDate", dynamicGridResultCriteria.ToWarrantyDate ?? (object)System.DBNull.Value),
                new SqlParameter("@FromFIDDate", dynamicGridResultCriteria.FromFIDDate ?? (object)System.DBNull.Value),
                new SqlParameter("@ToFIDDate", dynamicGridResultCriteria.ToFIDDate ?? (object)System.DBNull.Value),
                new SqlParameter("@FromFDDate", dynamicGridResultCriteria.FromFDDate ?? (object)System.DBNull.Value),
                new SqlParameter("@ToFDDate", dynamicGridResultCriteria.ToFDDate ?? (object)System.DBNull.Value),
                new SqlParameter("@PMContractList", dynamicGridResultCriteria.PMContractList ?? (object)System.DBNull.Value),
                new SqlParameter("@SortColumn", dynamicGridResultCriteria.SortColumn ?? (object)System.DBNull.Value),
                new SqlParameter("@SortOrder", dynamicGridResultCriteria.SortOrder ?? (object)System.DBNull.Value),
                new SqlParameter("@PageNo", dynamicGridResultCriteria.PageNo ?? (object)System.DBNull.Value),
                new SqlParameter("@PageSize", dynamicGridResultCriteria.PageSize ?? (object)System.DBNull.Value),
                new SqlParameter("@EmailAddress", dynamicGridResultCriteria.EmailAddress ?? (object)System.DBNull.Value),
                new SqlParameter("@FilterAlertBy", dynamicGridResultCriteria.FilterAlertBy ?? 0),
                new SqlParameter("@MachineStatusList", dynamicGridResultCriteria.MachineStatusList ?? (object)System.DBNull.Value),
                new SqlParameter("@LoginUserEmailAddress", dynamicGridResultCriteria.LoginUserEmailAddress ?? (object)System.DBNull.Value),
            };
            string query = $"Exec { Constants.StoreProcedures.MachineAlertCountDetails } @GlobalSearchtext,@IsGridFilterApplied,@PssrIdList,@CustomerIdList,@DistributorIdList,@BranchIdList,@ModelList,@SerialList," +
                $"@PINList,@MaxSMR,@MinSMR,@MaxMachineHealth,@MinMachineHealth,@FromWarrantyDate,@ToWarrantyDate,@FromFIDDate,@ToFIDDate,@FromFDDate,@ToFDDate,@PMContractList," +
                $"@SortColumn,@SortOrder,@PageNo,@PageSize,@EmailAddress,@FilterAlertBy,@MachineStatusList,@LoginUserEmailAddress";
            var result = await this.snmpContext.MachineGridAlert.MultipleResultsAsync(query, sqlParameter.ToArray<object>()).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Gets the machine health drill down information asynchronous.
        /// </summary>
        /// <param name="machineId">The machine identifier.</param>
        /// <param name="loggedInUserEmailId">The logged in user email identifier.</param>
        /// <returns>The <see cref="Task{IList{MachineHealthDrillDownInfo}}"/>.</returns>
        public async Task<IList<MachineHealthDrillDownInfo>> GetMachineHealthDrillDownInfoAsync(int machineId, string loggedInUserEmailId)
        {
            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@MachineId", machineId),
                new SqlParameter("@LoginUserEmailAddress", loggedInUserEmailId)
            };

            var result = await this.snmpContext.MachineHealthDrillDownInfo.MultipleResultsAsync($"Exec {Constants.StoreProcedures.GetMachineHealthDrillDownInfo} @MachineId", sqlParameter.ToArray<object>()).ConfigureAwait(false);
            return result;
        }

        /// <summary>
        /// Gets the matching machines asynchronous.
        /// </summary>
        /// <param name="primaryMachineId">The primary machine identifier.</param>
        /// <returns>The <see cref="Task{IList{LookupValue}}"/>.</returns>
        public async Task<IList<LookupValue>> GetMatchingMachinesAsync(int primaryMachineId)
        {
            _ = primaryMachineId <= 0 ? throw new ArgumentNullException(nameof(primaryMachineId)) : primaryMachineId;

            var sqlParameter = new List<SqlParameter>
            {
                new SqlParameter("@MachineId", primaryMachineId)
            };
            var result = await this.snmpContext.LookupValues.MultipleResultsAsync($"Exec {StoreProcedures.GetMatchingMachines} @MachineId", sqlParameter.ToArray<object>()).ConfigureAwait(false);

            return result;
        }

        /// <summary>
        /// The Mask Properties.
        /// </summary>
        /// <param name="machineMasterResult">The machineMasterResult<see cref="OrgIdListModel"/>.</param>
        /// <param name="orgIds">The orgIds<see cref="string"/>.</param>
        /// <param name="value">The value<see cref="MachineInfoModel"/>.</param>
        /// <returns>The <see cref="MachineInfoModel"/>.</returns>
        private static MachineInfoModel MaskProperties(OrgIdListModel machineMasterResult, string orgIds, MachineInfoModel value)
        {
            if (!machineMasterResult.CustomerNameOrgIds?.Exists(x => x.Contains(orgIds, StringComparison.OrdinalIgnoreCase)) ?? true)
            {
                if (string.IsNullOrEmpty(value.CustomerName) || value.CustomerName.Length < 3)
                {
                    value.CustomerName = KConstants.MASKEDASTERIC;
                }
                else
                {
                    value.CustomerName = value.CustomerName.Substring(value.CustomerName.Length - 3).PadLeft(value.CustomerName.Length, '*');
                }
            }

            if (!machineMasterResult.PersonalInfoOrgIds?.Exists(x => x.Contains(orgIds, StringComparison.OrdinalIgnoreCase)) ?? true)
            {
                if (string.IsNullOrEmpty(value.PssrName) || value.PssrName.Length < 3)
                {
                    value.PssrName = KConstants.MASKEDASTERIC;
                }
                else
                {
                    value.PssrName = value.PssrName.Substring(value.PssrName.Length - 3).PadLeft(value.PssrName.Length, '*');
                }
            }

            if (!machineMasterResult.MapOrgIds?.Exists(x => x.Contains(orgIds, StringComparison.OrdinalIgnoreCase)) ?? true)
            {
                value.Latitude = double.Parse("0", CultureInfo.InvariantCulture);
                value.Longitude = double.Parse("0", CultureInfo.InvariantCulture);
            }

            return value;
        }

        /// <summary>
        /// The ValidateMachineID.
        /// </summary>
        /// <param name="id">The id<see cref="string"/>.</param>
        /// <returns>The <see cref="string"/>.</returns>
        private static string ValidateMachineID(string id)
        {
            if (long.TryParse(id, out long number))
            {
                return number.ToString() == "0" ? string.Empty : number.ToString();
            }
            else
            {
                return string.Empty;
            }
        }

        /// <summary>
        /// Rebuilds the grid result criteria.
        /// </summary>
        /// <param name="criteria">The criteria.</param>
        /// <returns>Returns the new criteria.</returns>
        private static dynamic RebuildGridResultCriteria(GridResultCriteria criteria)
        {
            dynamic dynamicCriteria = new ExpandoObject();
            dynamicCriteria.GlobalSearchtext = null;
            dynamicCriteria.IsGridFilterApplied = null;
            dynamicCriteria.PssrIdList = null;
            dynamicCriteria.CustomerIdList = null;
            dynamicCriteria.DistributorIdList = null;
            dynamicCriteria.BranchIdList = null;
            dynamicCriteria.ModelList = null;
            dynamicCriteria.SerialList = null;
            dynamicCriteria.PINList = null;
            dynamicCriteria.MaxSMR = null;
            dynamicCriteria.MinSMR = null;
            dynamicCriteria.MaxMachineHealth = null;
            dynamicCriteria.MinMachineHealth = null;
            dynamicCriteria.FromWarrantyDate = null;
            dynamicCriteria.ToWarrantyDate = null;
            dynamicCriteria.FromFIDDate = null;
            dynamicCriteria.ToFIDDate = null;
            dynamicCriteria.FromFDDate = null;
            dynamicCriteria.ToFDDate = null;
            dynamicCriteria.PMContractList = null;
            dynamicCriteria.MachineStatusList = null;
            dynamicCriteria.LoginUserEmailAddress = null;
            dynamicCriteria.LoginUserCountry = null;
            dynamicCriteria.Role = null;
            if (criteria.Filters != null)
            {
                var filter = criteria.Filters;
                dynamicCriteria.GlobalSearchtext = filter.GlobalSearchtext;
                dynamicCriteria.IsGridFilterApplied = filter.IsGridFilterApplied;
                dynamicCriteria.PssrIdList = filter.PssrIdList;
                dynamicCriteria.CustomerIdList = filter.CustomerIdList;
                dynamicCriteria.ModelList = filter.ModelList;
                dynamicCriteria.SerialList = filter.SerialList;
                dynamicCriteria.PINList = filter.PinList;
                dynamicCriteria.MaxSMR = filter.MaxSmr;
                dynamicCriteria.MinSMR = filter.MinSmr;
                dynamicCriteria.MaxMachineHealth = filter.MaxMachineHealth;
                dynamicCriteria.MinMachineHealth = filter.MinMachineHealth;
                dynamicCriteria.FromWarrantyDate = filter.FromWarrantyDate;
                dynamicCriteria.ToWarrantyDate = filter.ToWarrantyDate;
                dynamicCriteria.FromFIDDate = filter.FromFidDate;
                dynamicCriteria.ToFIDDate = filter.ToFidDate;
                dynamicCriteria.FromFDDate = filter.FromFDDate;
                dynamicCriteria.ToFDDate = filter.ToFDDate;
                dynamicCriteria.PMContractList = filter.PMContractList;
                dynamicCriteria.MachineStatusList = filter.MachineStatusList;
                dynamicCriteria.DistributorIdList = filter.DistributorIdList;
                dynamicCriteria.BranchIdList = filter.BranchIdList;
                dynamicCriteria.Dealer = filter.Dealer ?? false;
            }

            dynamicCriteria.LoginUserEmailAddress = criteria.LoginUserEmailAddress;
            dynamicCriteria.LoginUserCountry = criteria.LoginUserCountry;
            dynamicCriteria.Role = criteria.Role;
            dynamicCriteria.SortColumn = criteria.SortField;
            dynamicCriteria.SortOrder = criteria.SortOrderString;
            dynamicCriteria.PageNo = criteria.PageNo;
            dynamicCriteria.PageSize = criteria.PageSize;
            dynamicCriteria.EmailAddress = criteria.EmailAddress;
            dynamicCriteria.FilterAlertBy = criteria.FilterAlertBy;

            return dynamicCriteria;
        }

        /// <summary>
        /// Gets the Set Machine Type.
        /// </summary>
        /// <param name="mt">The machine type.</param>
        /// <param name="oem">The OEM.</param>
        /// <returns>String <see cref="string"/>.</returns>
        private string SetMachineType(string mt, string oem)
        {
            if ((!string.IsNullOrEmpty(mt)) && (mt == Constants.MachineType.NonKomatsu))
            {
                return string.IsNullOrEmpty(oem) ? mt : oem;
            }

            if ((!string.IsNullOrEmpty(mt)) && (mt == Constants.MachineType.Komtrax || mt == Constants.MachineType.NonKomtrax || mt == Constants.MachineType.KPlus))
            {
                return mt;
            }

            return string.Empty;
        }
    }
}
