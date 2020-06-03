using DotNetty.Codecs;
using DotNetty.Handlers.Logging;
using DotNetty.Handlers.Timeout;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using log4net;
using log4net.Config;
using log4net.Repository;
using Microsoft.Extensions.DependencyInjection;
using NettyClinet.Models;
using System;
using System.IO;
using System.Net;
using System.Threading.Tasks;



namespace NettyClinet
{
    class Program
    {
        private static ILoggerRepository LoggerRepository;
        private static ILog log;
        static void Main(string[] args)
        {
            LoggerRepository = LogManager.CreateRepository("Log4netConsolePractice");
            XmlConfigurator.ConfigureAndWatch(LoggerRepository, new FileInfo("Log4net.config"));
            log = LogManager.GetLogger(LoggerRepository.Name, typeof(Program));
            log.Info("测试日志");
           // string path = ConfigHelper.Configuration["redisConn"];
            //if (!string.IsNullOrEmpty(path))
            //{
            //    Console.WriteLine(path);
            //}
            Console.WriteLine("Hello World!");
            RunClientAsync().Wait();
            //System.Diagnostics.Process.GetCurrentProcess().Kill();
        }

        static async Task RunClientAsync()
        {
            //Environment.SetEnvironmentVariable("io.netty.allocator.numDirectArenas","0");
            var group = new MultithreadEventLoopGroup();
            try
            {
                var bootstrap = new Bootstrap();
                bootstrap
                    .Group(group)
                    .Channel<TcpSocketChannel>()
                    .Option(ChannelOption.TcpNodelay, true)
                    .Handler(
                        new ActionChannelInitializer<ISocketChannel>(
                            channel =>
                            {
                                IChannelPipeline pipeline = channel.Pipeline;
                                pipeline.AddLast(new LoggingHandler());
                                // pipeline.AddLast("framing-enc", new LengthFieldPrepender(2));
                                pipeline.AddLast("framing-dec", new LengthFieldBasedFrameDecoder(int.MaxValue, 4, 4, 4, 0));
                                pipeline.AddLast(new IdleStateHandler(5, 0, 0));
                                pipeline.AddLast("echo", new ClientHandler(log));
                            }));
                IPAddress ip = IPAddress.Parse("128.6.2.46");
                IChannel clientChannel = await bootstrap.ConnectAsync(new IPEndPoint(ip, 9998));

                Console.ReadLine();
                await clientChannel.CloseAsync();
            }
            finally
            {
                await group.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
                Console.WriteLine("客户端优雅的释放了线程资源...");
            }
        }

        // static void Main() => RunClientAsync().Wait();

    }

}
