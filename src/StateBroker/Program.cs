using StateBroker.Core;

var configPath = args.Length > 0 ? args[0] : "appsettings.json";

var config = File.Exists(configPath)
    ? BrokerConfig.Load(configPath)
    : new BrokerConfig();

Console.WriteLine($"StateBroker starting on {config.Addr}");
Console.WriteLine($"  store.mode={config.Store?.Mode ?? "wal"}  cluster.mode={config.Cluster?.Mode ?? "single"}");
