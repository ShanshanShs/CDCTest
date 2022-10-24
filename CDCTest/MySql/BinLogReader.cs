using MySqlCdc;
using MySqlCdc.Constants;
using MySqlCdc.Events;
using MySqlCdc.Providers.MySql;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace CDCTest.MySql;

public class BinLogReader
{
    public async Task Start()
    {
        var client = new BinlogClient(options =>
        {
            options.Port = 3306;
            options.Username = "root";
            options.Password = "localdb123";
            options.SslMode = SslMode.Disabled;
            options.HeartbeatInterval = TimeSpan.FromSeconds(30);
            options.Blocking = true;

            /// Start replication from MySQL GTID. Recommended.
            var gtidSet = "43f3c65f-52da-11ed-b4a8-0242ac120002:1-5";
            options.Binlog = BinlogOptions.FromGtid(GtidSet.Parse(gtidSet));

            // Start replication from the master binlog filename and position
            //options.Binlog = BinlogOptions.FromPosition("mysql-bin.000008", 195);

            // Start replication from the master last binlog filename and position.
            options.Binlog = BinlogOptions.FromEnd();
        });

        await foreach (var binlogEvent in client.Replicate())
        {
            var state = client.State;

            if (binlogEvent is TableMapEvent tableMap)
            {
                HandleTableMapEvent(tableMap);
            }
            else if (binlogEvent is GtidEvent gtid)
            {
                await PrintEventAsync(binlogEvent);
            }
            else if (binlogEvent is WriteRowsEvent writeRows)
            {
                await HandleWriteRowsEvent(writeRows);
            }
            else if (binlogEvent is UpdateRowsEvent updateRows)
            {
                await HandleUpdateRowsEvent(updateRows);
            }
            else if (binlogEvent is DeleteRowsEvent deleteRows)
            {
                await HandleDeleteRowsEvent(deleteRows);
            }
            else await PrintEventAsync(binlogEvent);
        }
    }

    private static async Task PrintEventAsync(IBinlogEvent binlogEvent)
    {
        var json = JsonConvert.SerializeObject(binlogEvent, Formatting.Indented,
            new JsonSerializerSettings()
            {
                Converters = new List<JsonConverter> { new StringEnumConverter() }
            });
        await Console.Out.WriteLineAsync(json);
    }

    private static async Task HandleTableMapEvent(TableMapEvent tableMap)
    {
        Console.WriteLine($"Processing {tableMap.DatabaseName}.{tableMap.TableName}");
        await PrintEventAsync(tableMap);
    }

    private static async Task HandleWriteRowsEvent(WriteRowsEvent writeRows)
    {
        Console.WriteLine($"{writeRows.Rows.Count} rows were written");
        await PrintEventAsync(writeRows);

        foreach (var row in writeRows.Rows)
        {
            // Do something
        }
    }

    private static async Task HandleUpdateRowsEvent(UpdateRowsEvent updatedRows)
    {
        Console.WriteLine($"{updatedRows.Rows.Count} rows were updated");
        await PrintEventAsync(updatedRows);

        foreach (var row in updatedRows.Rows)
        {
            var rowBeforeUpdate = row.BeforeUpdate;
            var rowAfterUpdate = row.AfterUpdate;
            // Do something
        }
    }

    private static async Task HandleDeleteRowsEvent(DeleteRowsEvent deleteRows)
    {
        Console.WriteLine($"{deleteRows.Rows.Count} rows were deleted");
        await PrintEventAsync(deleteRows);

        foreach (var row in deleteRows.Rows)
        {
            // Do something
        }
    }
}