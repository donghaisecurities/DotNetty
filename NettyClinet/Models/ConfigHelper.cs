using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text;

namespace NettyClinet.Models
{
    public static class ConfigHelper
    {
        public static IConfigurationRoot Configuration { get; }

        public static string ProcessDirectory
        {
            get
            {
                return Directory.GetCurrentDirectory();

               // return AppContext.BaseDirectory;

                //return AppDomain.CurrentDomain.BaseDirectory;

            }
        }
        static ConfigHelper()
        {
            Configuration = new ConfigurationBuilder()
                 .SetBasePath(ProcessDirectory)
                 .AddJsonFile("appsettings.json")
                 .Build();
        }
    }
}
