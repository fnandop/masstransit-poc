using MassTransit;
using System.Configuration;
using System;

namespace CommonLegacy
{
    public sealed class IBusControlFactory
    {
        public static IBusControl CreateUsing(string transportName, Action<IReceiveConfigurator> configureAction = null)
        {
            switch (transportName)
            {
                case "RabbitMq": return CreateUsingRabbitMq(configureAction);
                case "SqlServer": return CreateUsingSqlServer(configureAction);
                default: throw new NotSupportedException($"The transport '{transportName}' is not supported.");

            }
        }

        private static IBusControl CreateUsingRabbitMq(Action<IReceiveConfigurator> configureAction)
        {
            var rabbitMqHost = ConfigurationManager.AppSettings["RabbitMq:Host"];
            var rabbitMqUsername = ConfigurationManager.AppSettings["RabbitMq:Username"];
            var rabbitMqPassword = ConfigurationManager.AppSettings["RabbitMq:Password"];

            return MassTransit.Bus.Factory.CreateUsingRabbitMq(cfg =>
            {
                cfg.Host(new Uri(rabbitMqHost), h =>
                {
                    h.Username(rabbitMqUsername);
                    h.Password(rabbitMqPassword);
                });
                configureAction?.Invoke(cfg);
            });
        }

        private static IBusControl CreateUsingSqlServer(Action<IReceiveConfigurator> configureAction)
        {
            var sqlConnectionString = ConfigurationManager.AppSettings["SqlServer:ConnectionString"];
            var sqlSchema = ConfigurationManager.AppSettings["SqlServer:Schema"];

            return SqlBusFactory.Create(cfg =>
            {
                cfg.UseSqlServer(sqlConnectionString, x =>
                {
                    x.Schema = sqlSchema;
                });
                configureAction?.Invoke(cfg);
            });
        }
    }


}
