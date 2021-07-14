using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos.Table;
using Newtonsoft.Json.Linq;

namespace Hslee.Function
{
    public static class ReadTable 
    {
        [FunctionName("ReadTable")]
        public static Task<String> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] 
        HttpRequest req, ILogger log, ExecutionContext context)
        {
            string connStra=Environment.GetEnvironmentVariable("AzureWebJobsStorage");
            string requestBody = new StreamReader(req.Body).ReadToEnd();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string PartitionKeyA=data.PartitionKey;
            string RowKeyA=data.RowKey;
            string contentA=data.Content;
            CloudStorageAccount sta = CloudStorageAccount.Parse(connStra);
            CloudTableClient tbC=sta.CreateCloudTableClient();
            CloudTable tableA=tbC.GetTableReference("tableA");

            string filterA=TableQuery.GenerateFilterCondition("PartitionKey",QueryComparisons.GreaterThanOrEqual, PartitionKeyA);
            string filterB=TableQuery.GenerateFilterCondition("RowKey",QueryComparisons.GreaterThanOrEqual, RowKeyA);
            Task<string> response=ReadToTable(tableA, filterA, filterB);
            return response;
       }
        static async Task<string> ReadToTable(CloudTable tableA, string filterA, string filterB) 
        {
            TableQuery<MemoData> rangeQ=new TableQuery<MemoData>().Where(TableQuery.CombineFilters(filterA, TableOperators.And, filterB));
            TableContinuationToken tokenA = null;
            JArray resultArr = new JArray();
            rangeQ.TakeCount=1000;
            try
            {
                do
                {
                    TableQuerySegment<MemoData> segment= await tableA.ExecuteQuerySegmentedAsync(rangeQ, tokenA);
                    tokenA=segment.ContinuationToken;
                    foreach(MemoData entity in segment)
                    {
                        JObject srcObj=JObject.FromObject(entity);
                        //srcObj.Remove("Timestamp");
                        resultArr.Add(srcObj);
                    }
                } while(tokenA != null );
            }
            catch (StorageException se)
            {
                Console.WriteLine(se.Message);
                
                throw;
            }
            string resultA = Newtonsoft.Json.JsonConvert.SerializeObject(resultArr);
            if(resultA !=null) return resultA;
            else return "No Data";
        }

        private class MemoData: TableEntity
        {
            public string content{get; set;}
        }
    }
}