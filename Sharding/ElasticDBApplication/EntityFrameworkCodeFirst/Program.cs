// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Data.SqlClient;
using System.Linq;
using EFCodeFirstElasticScale;
using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

////////////////////////////////////////////////////////////////////////////////////////
// This sample follows the CodeFirstNewDatabase Blogging tutorial for EF.
// It illustrates the adjustments that need to be made to use EF in combination
// with the Entity Framewor to scale out your data tier across many databases and
// benefit from Elastic Scale capabilities for Data Dependent Routing and 
// Shard Map Management.
////////////////////////////////////////////////////////////////////////////////////////

namespace EFCodeFirstElasticScale
{
    // This sample requires three pre-created empty SQL Server databases. 
    // The first database serves as the shard map manager database to store the Elastic Scale shard map.
    // The remaining two databases serve as shards to hold the data for the sample.
    internal class Program
    {
        // You need to adjust the following settings to your database server and database names in Azure Db
           private static string s_server = "";
        private static string s_shardmapmgrdb = "[YourShardMapManagerDatabaseName]";
        private static string s_shard1 = "Company1";
        private static string s_shard2 = "Company2";
        private static string s_userName = "";
        private static string s_password = "";
        private static string s_applicationName = "ESC_EFv1.0";

        // Just two tenants for now.
        // Those we will allocate to shards.
        private static int s_tenantId1 = 1;
        private static int s_tenantId2 = 2;
        private static ShardMapManager s_shardMapManager;

        public static void Main()
        {
            // Get the shard map manager, if it already exists.
            // It is recommended that you keep only one shard map manager instance in
            // memory per AppDomain so that the mapping cache is not duplicated.
            s_shardMapManager = ShardManagementUtils.TryGetShardMapManager(
                Configuration.ShardMapManagerServerName,
                "Consero_ShardMapManagerDb");

            // CreateShardMapManagerAndShard();

            MultiShardQuery();
            
            Console.WriteLine("Press any key to exit...");
            Console.ReadKey();
        }
        private static ListShardMap<int> TryGetShardMap()
        {
            
            ListShardMap<int> shardMap;
            bool mapExists = s_shardMapManager.TryGetListShardMap("ElasticScaleWithEF", out shardMap);

            if (!mapExists)
            {
               // ConsoleUtils.WriteWarning("Shard Map Manager has been created, but the Shard Map has not been created");
                return null;
            }

            return shardMap;
        }

        private static void MultiShardQuery()
        {
            ListShardMap<int> shardMap = TryGetShardMap();
            if (shardMap != null)
            {
                MultiShardQuerySample.ExecuteMultiShardQuery(
                    shardMap,
                    Configuration.GetCredentialsConnectionString());
            }
        }

        private static void CreateShardMapManagerAndShard()
        {
            SqlConnectionStringBuilder connStrBldr = new SqlConnectionStringBuilder
            {
                UserID = s_userName,
                Password = s_password,
                ApplicationName = s_applicationName
            };
            Console.WriteLine("Checking for existing shard map and creating new shard map if necessary.");

            // Create shard map manager database
            if (!SqlDatabaseUtils.DatabaseExists(Configuration.ShardMapManagerServerName, Configuration.ShardMapManagerDatabaseName))
            {
                SqlDatabaseUtils.CreateDatabase(Configuration.ShardMapManagerServerName, Configuration.ShardMapManagerDatabaseName);
            }

            // Create shard map manager
            //string shardMapManagerConnectionString =
            //    Configuration.GetConnectionString(
            //        Configuration.ShardMapManagerServerName,
            //        Configuration.ShardMapManagerDatabaseName);

            //s_shardMapManager = ShardManagementUtils.CreateOrGetShardMapManager(shardMapManagerConnectionString);


            Sharding sharding = new Sharding(s_server, Configuration.ShardMapManagerDatabaseName, connStrBldr.ConnectionString);
            sharding.RegisterNewShard(s_server, s_shard1, connStrBldr.ConnectionString, s_tenantId1);
            sharding.RegisterNewShard(s_server, s_shard2, connStrBldr.ConnectionString, s_tenantId2);

            //// Create shard map
            //RangeShardMap<int> shardMap = ShardManagementUtils.CreateOrGetRangeShardMap<int>(
            //    s_shardMapManager, Configuration.ShardMapName);

            //// Create schema info so that the split-merge service can be used to move data in sharded tables
            //// and reference tables.
            ////CreateSchemaInfo(shardMap.Name);

            //// If there are no shards, add two shards: one for [0,100) and one for [100,+inf)
            //if (!shardMap.GetShards().Any())
            //{
            //    CreateShardSample.CreateShard(shardMap, new Range<int>(0, 100));
            //    CreateShardSample.CreateShard(shardMap, new Range<int>(100, 200));
            //}
            Console.Write("Enter a name for a new Blog for CMS DB: ");
            var name5 = Console.ReadLine();

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var CMSDB = new ElasticScaleContext<int>())
                {
                    var blog = new Blog { Name = name5 };
                    CMSDB.Blogs.Add(blog);
                    CMSDB.SaveChanges();
                }
            });

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>())
                {
                    // Display all Blogs for tenant 1
                    var query = from b in db.Blogs
                                orderby b.Name
                                select b;

                    Console.WriteLine("All blogs for CMS DB");
                    foreach (var item in query)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });


            Console.Write("Enter a name for a new Blog: ");
            var name = Console.ReadLine();

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId1, connStrBldr.ConnectionString))
                {
                    var blog = new Blog { Name = name };
                    db.Blogs.Add(blog);
                    db.SaveChanges();
                }
            });

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId1, connStrBldr.ConnectionString))
                {
                    // Display all Blogs for tenant 1
                    var query = from b in db.Blogs
                                orderby b.Name
                                select b;

                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId1);
                    foreach (var item in query)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

            // Do work for tenant 2 :-)
            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId2, connStrBldr.ConnectionString))
                {
                    // Display all Blogs from the database 
                    var query = from b in db.Blogs
                                orderby b.Name
                                select b;

                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                    foreach (var item in query)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

            // Create and save a new Blog 
            Console.Write("Enter a name for a new Blog: ");
            var name2 = Console.ReadLine();

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId2, connStrBldr.ConnectionString))
                {
                    var blog = new Blog { Name = name2 };
                    db.Blogs.Add(blog);
                    db.SaveChanges();
                }
            });

            SqlDatabaseUtils.SqlRetryPolicy.ExecuteAction(() =>
            {
                using (var db = new ElasticScaleContext<int>(sharding.ShardMap, s_tenantId2, connStrBldr.ConnectionString))
                {
                    // Display all Blogs from the database 
                    var query = from b in db.Blogs
                                orderby b.Name
                                select b;

                    Console.WriteLine("All blogs for tenant id {0}:", s_tenantId2);
                    foreach (var item in query)
                    {
                        Console.WriteLine(item.Name);
                    }
                }
            });

        }

    }
}
