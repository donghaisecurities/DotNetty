using DotNetty.Buffers;
using DotNetty.Common.Utilities;
using DotNetty.Handlers.Timeout;
using DotNetty.Transport.Channels;
using log4net;
using NettyClinet.Models;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace NettyClinet
{
    public class ClientHandler : ChannelHandlerAdapter
    {
        private static RedisHelper redis;
        //private static RedisHelper redisAsync;
        private static Dictionary<long, Msg300192_011> cjdBuy;
        private static Dictionary<long, Msg300192_011> cjdSell;
        private static Dictionary<long, Msg300192_011> sddBuy;
        private static Dictionary<long, Msg300192_011> sddSell;
        private static Dictionary<long, Msg300192_011> tljdBuy;
        private static Dictionary<long, Msg300192_011> tljdSell;
        private static Dictionary<long, Msg300192_011> zl;

        private static List<Msg300192_011> tljdList;
        private static LockFreeQueue<IByteBuffer> lockFreeQueue;


        readonly IByteBuffer initialMessage;
        private ILog log;
        //4
        int msgType = 1;
        // char[20]
        string senderCompID = ConfigHelper.Configuration["senderCompID"];
        // char[20]
        string targetCompID = ConfigHelper.Configuration["targetCompID"];
        //4
        int heartBtInt = 60;
        // char[16]
        string password = ConfigHelper.Configuration["password"];
        // char[32]
        string defaultApplVerID = "1.01";
        // ConfigHelper.Configuration["redisDB"]

        public int GenerateCheckSum(IByteBuffer buffer, int len)
        {
            byte[] bf = buffer.Array;

            int cks = 0;

            for (int idx = 0; idx < len; idx++)
            {
                cks += bf[idx];
            }

            return cks % 256;
        }

        private static void QueueLevel2()
        {
            try
            {

                while (true)
                {

                    var byteBuffer = lockFreeQueue.Dequeue();

                    //if (byteBuffer != null)
                    //{
                    //    //Console.WriteLine(byteBuffer.ReferenceCount);
                    //    byteBuffer.Release();
                    //}
                    //else
                    //{
                    //    Console.WriteLine("空对象");
                    //}


                    if (byteBuffer != null)
                    {
                        int msgType = byteBuffer.ReadInt();//4
                        if (msgType == 300192)
                        {
                            int bodyLength = byteBuffer.ReadInt();//4
                            int channelNo = byteBuffer.ReadShort();//频道代码//2
                            long applSeqNum = byteBuffer.ReadLong(); //消息记录号 从 1开始计数 //8
                            string mdStreamID = byteBuffer.ReadString(3, Encoding.UTF8).Trim();//行情类别3
                            string securityID = byteBuffer.ReadString(8, Encoding.UTF8).Trim();//证券代码8
                            string securityIDSource = byteBuffer.ReadString(4, Encoding.UTF8).Trim();//证券代码源4


                            double price = byteBuffer.ReadLong();//8
                            double orderQty = byteBuffer.ReadLong();//8
                            string side = byteBuffer.ReadString(1, Encoding.UTF8).Trim();//1
                            long transactTime = byteBuffer.ReadLong();//8
                            string ordType = byteBuffer.ReadString(1, Encoding.UTF8).Trim();//扩展字段-订单类型1

                            var msg = new Msg300192_011()
                            {
                                ChannelNo = channelNo,
                                ApplSeqNum = applSeqNum,
                                MdStreamID = mdStreamID,
                                SecurityID = securityID,
                                SecurityIDSource = securityIDSource,
                                Price = price,
                                OrderQty = orderQty,
                                Side = side,
                                TransactTime = transactTime,
                            };

                            if (securityID.StartsWith("00") || securityID.StartsWith("30"))
                            {
                                //cjdThread.Start(msg);
                                string tradeDay = transactTime.ToString().Substring(0, 8);


                                //主力资金
                                //60000股||20万元
                                if ((orderQty / 100.0 >= 60000.0 || (price / 10000.0 * orderQty / 100.0) >= 200000.0))
                                {
                                    if (zl.ContainsKey(applSeqNum))
                                    {
                                        zl[applSeqNum] = msg;
                                        //Console.WriteLine(zl[applSeqNum].ApplSeqNum + "|" + zl[applSeqNum].SecurityID + "|" + zl[applSeqNum].OrderQty + "|" + zl[applSeqNum].Price);
                                        //Console.WriteLine(msg.ApplSeqNum + "|" + msg.SecurityID + "|" + msg.OrderQty + "|" + msg.Price);
                                        //Console.WriteLine("----------------");
                                    }
                                    else
                                    {
                                        zl.Add(applSeqNum, msg);
                                    }

                                    // redis.HashSetAsync("zlwt_" + tradeDay + "_" + securityID, applSeqNum.ToString(), msg);//单只股票委托记录                                    
                                    _ = redis.HashIncrementAsync("zlwt_" + tradeDay, securityID);
                                }


                                //闪电单 市价委托&& 60000股||20万元
                                if (ordType == "1" && (orderQty / 100.0 >= 60000.0 || (price / 10000.0 * orderQty / 100.0) >= 200000.0))
                                {
                                    _ = redis.HashSetAsync("wt_" + tradeDay + "_sdd_" + securityID, applSeqNum.ToString(), msg);//委托记录
                                    if (side == "1")
                                    {
                                        sddBuy.Add(applSeqNum, msg);
                                        _ = redis.HashIncrementAsync("sdd_" + tradeDay + "_buy", securityID);
                                    }
                                    else
                                    {
                                        sddSell.Add(applSeqNum, msg);
                                        _ = redis.HashIncrementAsync("sdd_" + tradeDay + "_sell", securityID);
                                    }
                                }

                                //超级单 9999手
                                if ((orderQty / 10000.0) >= 9999.0)
                                {
                                    // Console.WriteLine(dicBuy.Count);
                                    // Console.WriteLine(applSeqNum + "|" + securityID + "|" + price / 10000 + "|" + orderQty / 10000 + "|" + side + "|" + transactTime);
                                    _ = redis.HashSetAsync("wt_" + tradeDay + "_cjd_" + securityID, applSeqNum.ToString(), msg);//委托记录
                                    if (side == "1")
                                    {
                                        cjdBuy.Add(applSeqNum, msg);
                                        _ = redis.HashIncrementAsync("cjd_" + tradeDay + "_buy", securityID);
                                    }
                                    else
                                    {
                                        cjdSell.Add(applSeqNum, msg);
                                        _ = redis.HashIncrementAsync("cjd_" + tradeDay + "_sell", securityID);
                                    }

                                }
                                ///拖拉机单
                                if (tljdList.Count > 0)
                                {
                                    //金额大于100万单子才选，否则委托号不连续导致不认定为拖拉机单
                                    if (((msg.Price / 10000.0) * (msg.OrderQty / 100.0)) >= 1000000.0)
                                    {
                                        var lastMsg = tljdList[tljdList.Count - 1];
                                        //连续委托中 证券代码、买卖方向、委托数量相同且金额大于100万
                                        if (lastMsg.SecurityID == msg.SecurityID && lastMsg.OrderQty == msg.OrderQty && lastMsg.Side == msg.Side)
                                        {
                                            // Console.WriteLine(applSeqNum + "|" + securityID + "|" + price / 10000.0 + "|" + orderQty / 100.0 + "|" + side + "|" + transactTime);

                                            tljdList.Add(msg);
                                            // Console.WriteLine("tljdList.Count:" + tljdList.Count);
                                        }
                                        else
                                        {
                                            if (tljdList.Count >= 4)
                                            {
                                                string itemSide = "1";
                                                string itemSecid = "";
                                                foreach (var item in tljdList)
                                                {
                                                    Console.WriteLine(item.ApplSeqNum + "|" + item.SecurityID + "|" + item.Price / 10000.0 + "|" + item.OrderQty / 100.0 + "|" + item.Side + "|" + item.TransactTime);
                                                    itemSide = item.Side;
                                                    itemSecid = item.SecurityID;
                                                    //委托单存起来
                                                    _ = redis.HashSetAsync("wt_" + tradeDay + "_tljd_" + item.SecurityID, item.ApplSeqNum.ToString(), item);//委托记录
                                                    if (itemSide == "1")
                                                    {
                                                        tljdBuy.Add(item.ApplSeqNum, item);
                                                    }
                                                    else
                                                    {
                                                        tljdSell.Add(item.ApplSeqNum, item);
                                                    }
                                                }
                                                if (itemSide == "1")
                                                {
                                                    _ = redis.HashIncrementAsync("tljd_" + tradeDay + "_buy", itemSecid);
                                                }
                                                else
                                                {
                                                    _ = redis.HashIncrementAsync("tljd_" + tradeDay + "_sell", itemSecid);
                                                }
                                                Console.WriteLine("分割线-------------------------------");
                                            }
                                            tljdList.Clear();
                                            tljdList.Add(msg);
                                        }
                                    }
                                }
                                else
                                {
                                    tljdList.Add(msg);
                                }
                            }

                        }

                        if (msgType == 300191)
                        {
                            int bodyLength = byteBuffer.ReadInt();//4
                            int channelNo = byteBuffer.ReadShort();//频道代码//2
                            long applSeqNum = byteBuffer.ReadLong(); //消息记录号 从 1开始计数 //8
                            string mdStreamID = byteBuffer.ReadString(3, Encoding.UTF8).Trim();//行情类别3
                            long bidApplSeqNum = byteBuffer.ReadLong();//买方委托索引8
                            long offerApplSeqNum = byteBuffer.ReadLong();//卖方委托索引8
                            string securityID = byteBuffer.ReadString(8, Encoding.UTF8).Trim();//证券代码8
                            string securityIDSource = byteBuffer.ReadString(4, Encoding.UTF8).Trim();//证券代码源4


                            double lastPx = byteBuffer.ReadLong();//8
                            double lastQty = byteBuffer.ReadLong();//8
                            string execType = byteBuffer.ReadString(1, Encoding.UTF8).Trim();//1
                            long transactTime = byteBuffer.ReadLong();//8

                            //var msg = new Msg300191_011()
                            //{
                            //    ChannelNo = channelNo,
                            //    MdStreamID = mdStreamID,
                            //    ApplSeqNum = applSeqNum,
                            //    BidApplSeqNum = bidApplSeqNum,
                            //    OfferApplSeqNum = offerApplSeqNum,
                            //    SecurityID = securityID,
                            //    SecurityIDSource = securityIDSource,
                            //    LastPx = lastPx,
                            //    LastQty = lastQty,
                            //    ExecType = execType,
                            //    TransactTime = transactTime,
                            //};

                            if ((securityID.StartsWith("00") || securityID.StartsWith("30")) && execType == "F")
                            {
                                string tradeDay = transactTime.ToString().Substring(0, 8);
                                string cjTime = transactTime.ToString();//成交时间
                                double qtyPri = lastPx * lastQty;


                                bool zlBuy = false;
                                ///主力资金
                                if (zl.ContainsKey(bidApplSeqNum))//买委托单
                                {
                                    var obj = zl[bidApplSeqNum];
                                    if (obj.SecurityID == securityID)
                                    {
                                        zlBuy = true;

                                        //成交金额累加                  
                                        //  Console.WriteLine(bidApplSeqNum);
                                        // redis.HashIncrementAsync("zlcj_" + tradeDay + "_buy_" + securityID, zl[bidApplSeqNum].ApplSeqNum.ToString(), qtyPri);
                                        _ = redis.HashIncrementAsync("zlcj_" + tradeDay, securityID, qtyPri);//主力成交
                                        _ = redis.HashIncrementAsync("zljmr_" + tradeDay, securityID, qtyPri);//主力净买入
                                        if (obj.OrderQty == lastQty)//委托数量等于成交数量
                                            zl.Remove(bidApplSeqNum);

                                    }
                                }
                                ///主力资金
                                if (zl.ContainsKey(offerApplSeqNum))//卖委托单
                                {
                                    var obj = zl[offerApplSeqNum];
                                    if (obj.SecurityID == securityID)
                                    {
                                        // Console.WriteLine(offerApplSeqNum);
                                        //成交金额累加                                   
                                        // redis.HashIncrementAsync("zlcj_" + tradeDay + "_sell_" + securityID, zl[offerApplSeqNum].ApplSeqNum.ToString(), qtyPri);
                                        if (!zlBuy)
                                        {
                                            //一只股票买单已经算过，卖单主力成交就不要再算一遍了
                                            _ = redis.HashIncrementAsync("zlcj_" + tradeDay, securityID, qtyPri);//主力成交
                                        }
                                        _ = redis.HashDecrementAsync("zljmr_" + tradeDay, securityID, qtyPri); //主力净买入，减去卖单
                                        if (obj.OrderQty == lastQty)//委托数量等于成交数量
                                            zl.Remove(offerApplSeqNum);

                                    }
                                }


                                //*************************/超级单


                                if (cjdBuy.ContainsKey(bidApplSeqNum))//买委托单
                                {
                                    var obj = cjdBuy[bidApplSeqNum];
                                    if (obj.SecurityID == securityID)
                                    {
                                        // Console.WriteLine(qtyPri);
                                        //成交单在超级委托里面
                                        //  Console.WriteLine(bidApplSeqNum + "|" + dicBuy[bidApplSeqNum].OrderQty + "|" + lastPx + "|" + lastQty + "|" + execType + "|" + applSeqNum);
                                        //成交金额累加                                   
                                        _ = redis.HashIncrementAsync("cjd_" + tradeDay + "_buy_pri", securityID, qtyPri);

                                        var msg = new
                                        {
                                            SecurityID = securityID,
                                            WtTime = obj.TransactTime,//委托时间
                                            CjTime = cjTime,
                                            WtSide = obj.Side,//
                                            WtSeqNum = bidApplSeqNum,//委托单号
                                            WtPx = obj.Price,
                                            WtQty = obj.OrderQty,
                                            CjPx = lastPx,
                                            CjQty = lastQty,
                                        };
                                        // redis.HashSet("list_cjd_" + tradeDay, tradeTime, msg);
                                        _ = redis.HashSetAsync("cj_" + tradeDay + "_cjd_" + securityID, applSeqNum.ToString(), msg);//成交记录
                                    }
                                }
                                if (cjdSell.ContainsKey(offerApplSeqNum))//卖委托单
                                {
                                    var obj = cjdSell[offerApplSeqNum];
                                    if (obj.SecurityID == securityID)
                                    {
                                        // Console.WriteLine(qtyPri);
                                        //成交单在超级委托里面
                                        // Console.WriteLine(offerApplSeqNum + "|" + dicSell[offerApplSeqNum].OrderQty + "|" + lastPx + "|" + lastQty + "|" + execType + "|" + applSeqNum);
                                        _ = redis.HashIncrementAsync("cjd_" + tradeDay + "_sell_pri", securityID, qtyPri);

                                        var msg = new
                                        {
                                            SecurityID = securityID,
                                            WtTime = obj.TransactTime,//委托时间
                                            CjTime = transactTime,
                                            WtSide = obj.Side,
                                            WtSeqNum = offerApplSeqNum,
                                            WtPx = obj.Price,
                                            WtQty = obj.OrderQty,
                                            CjPx = lastPx,
                                            CjQty = lastQty,
                                        };
                                        //redis.HashSet("list_cjd_" + tradeDay, tradeTime, msg);
                                        _ = redis.HashSetAsync("cj_" + tradeDay + "_cjd_" + securityID, applSeqNum.ToString(), msg);//成交记录
                                    }
                                }



                                //*************************/闪电单
                                if (sddBuy.ContainsKey(bidApplSeqNum))//买委托单
                                {
                                    var obj = sddBuy[bidApplSeqNum];
                                    // Console.WriteLine(qtyPri);
                                    //成交单在闪电单委托里面
                                    if (obj.SecurityID == securityID)
                                    {
                                        _ = redis.HashIncrementAsync("sdd_" + tradeDay + "_buy_pri", securityID, qtyPri);

                                        var msg = new
                                        {
                                            SecurityID = securityID,
                                            WtTime = obj.TransactTime,//委托时间
                                            CjTime = transactTime,
                                            WtSide = obj.Side,
                                            WtSeqNum = bidApplSeqNum,
                                            WtPx = obj.Price,
                                            WtQty = obj.OrderQty,
                                            CjPx = lastPx,
                                            CjQty = lastQty,
                                        };
                                        //  redis.HashSet("list_sdd_" + tradeDay, tradeTime, msg);
                                        _ = redis.HashSetAsync("cj_" + tradeDay + "_sdd_" + securityID, applSeqNum.ToString(), msg);//成交记录
                                    }
                                }

                                if (sddSell.ContainsKey(offerApplSeqNum))//卖委托单
                                {
                                    var obj = sddSell[offerApplSeqNum];
                                    // Console.WriteLine(qtyPri);
                                    //成交单在闪电单委托里面
                                    if (obj.SecurityID == securityID)
                                    {
                                        _ = redis.HashIncrementAsync("sdd_" + tradeDay + "_sell_pri", securityID, qtyPri);

                                        var msg = new
                                        {
                                            SecurityID = securityID,
                                            WtTime = obj.TransactTime,//委托时间
                                            CjTime = transactTime,
                                            WtSide = obj.Side,
                                            WtSeqNum = offerApplSeqNum,
                                            WtPx = obj.Price,
                                            WtQty = obj.OrderQty,
                                            CjPx = lastPx,
                                            CjQty = lastQty,
                                        };
                                        //redis.HashSet("list_sdd_" + tradeDay, tradeTime, msg);
                                        _ = redis.HashSetAsync("cj_" + tradeDay + "_sdd_" + securityID, applSeqNum.ToString(), msg);//成交记录
                                    }
                                }



                                //*************************/拖拉机单

                                if (tljdBuy.ContainsKey(bidApplSeqNum))//买委托单
                                {
                                    //还要判断证券代码是否变化,否则 已经撤单，委托号变为其他股票的委托
                                    var obj = tljdBuy[bidApplSeqNum];
                                    if (obj.SecurityID == securityID)
                                    {
                                        _ = redis.HashIncrementAsync("tljd_" + tradeDay + "_buy_pri", securityID, qtyPri);

                                        var msg = new
                                        {
                                            SecurityID = securityID,
                                            WtTime = obj.TransactTime,//委托时间
                                            CjTime = transactTime,
                                            WtSide = obj.Side,
                                            WtSeqNum = bidApplSeqNum,
                                            WtPx = obj.Price,
                                            WtQty = obj.OrderQty,
                                            CjPx = lastPx,
                                            CjQty = lastQty,
                                        };
                                        //redis.HashSet("list_tljd_" + tradeDay, tradeTime, msg);
                                        _ = redis.HashSetAsync("cj_" + tradeDay + "_tljd_" + securityID, applSeqNum.ToString(), msg);//成交记录
                                    }
                                }


                                if (tljdSell.ContainsKey(offerApplSeqNum))//卖委托单
                                {
                                    var obj = tljdSell[offerApplSeqNum];

                                    if (obj.SecurityID == securityID)
                                    {
                                        _ = redis.HashIncrementAsync("tljd_" + tradeDay + "_sell_pri", securityID, qtyPri);

                                        var msg = new
                                        {
                                            SecurityID = securityID,
                                            WtTime = obj.TransactTime,//委托时间
                                            CjTime = transactTime,
                                            WtSide = obj.Side,
                                            WtSeqNum = offerApplSeqNum,
                                            WtPx = obj.Price,
                                            WtQty = obj.OrderQty,
                                            CjPx = lastPx,
                                            CjQty = lastQty,
                                        };
                                        // redis.HashSet("list_tljd_" + tradeDay, tradeTime, msg);
                                        _ = redis.HashSetAsync("cj_" + tradeDay + "_tljd_" + securityID, applSeqNum.ToString(), msg);//成交记录
                                    }
                                }

                            }

                        }

                        if (msgType == 300111)
                        {
                            int bodyLength = byteBuffer.ReadInt();//4
                            long origTime = byteBuffer.ReadLong();//数据生成时间8
                            int channelNo = byteBuffer.ReadShort();//频道代码//2
                            string mdStreamID = byteBuffer.ReadString(3, Encoding.UTF8).Trim();//行情类别3
                            string securityID = byteBuffer.ReadString(8, Encoding.UTF8).Trim();//证券代码8

                            if (securityID.StartsWith("00") || securityID.StartsWith("30"))
                            {
                                string securityIDSource = byteBuffer.ReadString(4, Encoding.UTF8).Trim();//证券代码源4
                                string tradingPhaseCode = byteBuffer.ReadString(8, Encoding.UTF8).Trim();//交易状态8
                                double prevClose = byteBuffer.ReadLong();//8 昨收盘
                                double numTrades = byteBuffer.ReadLong();//8  成交笔数
                                double totalVolumeTrade = byteBuffer.ReadLong();//8 成交总量 2位小数
                                double totalValueTrade = byteBuffer.ReadLong();//8 成交总金额 4位小数

                                var obj = new
                                {
                                    origTime,
                                    securityID,
                                    tradingPhaseCode,
                                    prevClose,
                                    numTrades,
                                    totalVolumeTrade,
                                    totalValueTrade,
                                };
                                _ = redis.StringSetAsync("gghq_" + securityID, obj);
                                //Console.WriteLine(origTime + "|" + channelNo + "|" + mdStreamID + "|" + tradingPhaseCode + "|" + prevClose + "|" + totalVolumeTrade);
                            }
                        }


                        if (msgType == 3)
                        {
                            // Console.WriteLine(msgType);
                        }
                        if (msgType == 1)//登录成功返回的消息
                        {
                            //int bodyLength = byteBuffer.ReadInt();

                            //string senderCompID = byteBuffer.ReadString(20, Encoding.UTF8);
                            //string targetCompID = byteBuffer.ReadString(20, Encoding.UTF8);
                            //int heartBtInt = byteBuffer.ReadInt();
                            //string password = byteBuffer.ReadBytes(16).ToString(Encoding.UTF8);
                            //string defaultApplVerID = byteBuffer.ReadBytes(32).ToString(Encoding.UTF8);
                            //int checkSum = byteBuffer.ReadInt();

                            //Console.WriteLine(msgType+" "+bodyLength + " " + senderCompID + " " + password + " " + checkSum);
                        }


                        byteBuffer.Release();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("读取队列数据：" + ex.Message);
            }

        }

        public ClientHandler(ILog log)
        {
            cjdBuy = new Dictionary<long, Msg300192_011>();
            cjdSell = new Dictionary<long, Msg300192_011>();
            sddBuy = new Dictionary<long, Msg300192_011>();
            sddSell = new Dictionary<long, Msg300192_011>();
            tljdBuy = new Dictionary<long, Msg300192_011>();
            tljdSell = new Dictionary<long, Msg300192_011>();
            tljdList = new List<Msg300192_011>();
            zl = new Dictionary<long, Msg300192_011>();

            lockFreeQueue = new LockFreeQueue<IByteBuffer>();
            // redis = new RedisHelper(9, "128.6.5.10:6379,allowadmin=true");//包含DBNub,port
            redis = new RedisHelper(Convert.ToInt32(ConfigHelper.Configuration["redisDB"]), ConfigHelper.Configuration["redisConn"]);//包含DBNub,port
                                                                                                                                     // redisAsync = new RedisHelper(Convert.ToInt32(ConfigHelper.Configuration["redisDBAsync"]), ConfigHelper.Configuration["redisConnAsync"]);//包含DBNub,port
                                                                                                                                     //redis = new RedisHelper(0, "128.6.2.85:6380,password = 123456");//包含DBNub,port

            this.log = log;
            //short[] shorts = new short[10];
            // Buffer.SetByte(shorts, 0, Encoding.UTF8.GetBytes("1"));
            // Console.WriteLine(Buffer);

            //IByteBuffer bf = this.initialMessage.Allocator.Buffer(100);

            this.initialMessage = Unpooled.Buffer(100);
            // byte[] messageBytes = Encoding.UTF8.GetBytes("Hello world");
            // this.initialMessage.WriteBytes(messageBytes);


            //4
            this.initialMessage.WriteInt(msgType);
            //4
            this.initialMessage.WriteInt(92);
            //20
            this.initialMessage.WriteString(senderCompID, Encoding.UTF8);
            this.initialMessage.SetWriterIndex(28);
            //20
            this.initialMessage.WriteString(targetCompID, Encoding.UTF8);
            this.initialMessage.SetWriterIndex(48);
            //4
            this.initialMessage.WriteInt(heartBtInt);
            // this.initialMessage.SetWriterIndex(52);
            //16
            this.initialMessage.WriteString(password, Encoding.UTF8);
            this.initialMessage.SetWriterIndex(68);
            //32
            this.initialMessage.WriteString(defaultApplVerID, Encoding.UTF8);
            this.initialMessage.SetWriterIndex(100);

            this.initialMessage.WriteInt(GenerateCheckSum(this.initialMessage, 100));

            Thread threadLevel2 = new Thread(new ThreadStart(QueueLevel2));
            threadLevel2.Start();
            Thread lockFreeCount = new Thread(new ThreadStart(GetlockFreeCount));
            lockFreeCount.Start();
        }

        public override void ChannelActive(IChannelHandlerContext context) => context.WriteAndFlushAsync(this.initialMessage);

        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            var byteBuffer = message as IByteBuffer;

            try
            {
                if (byteBuffer != null)
                    lockFreeQueue.EnQueue(byteBuffer);
            }
            catch (Exception ex)
            {
                Console.WriteLine("入无锁队列：" + ex.Message);
            }
            finally
            {
                // byteBuffer.Release();
                //byteBuffer.Retain();
                //ReferenceCountUtil.Release(message);//防止内存泄漏
                //if (byteBuffer.ReferenceCount > 0)
                //{
                //    byteBuffer.Release();
                //}
            }

        }


        public override void ChannelReadComplete(IChannelHandlerContext context)
        {
            // input.Subscribe((p => Console.WriteLine($"订阅：{p}")));
            //msg300192_011.Subscribe((p => Console.WriteLine(p.ChannelNo + "|" + p.SecurityID + "|" + p.Price + "|" + p.OrderQty + "|" + p.Side + "|" + p.TransactTime)));

            context.Flush();
        }

        /// <summary>
        /// 主动和被动与服务器断开后触发
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelInactive(IChannelHandlerContext context)
        {
            Console.WriteLine("与服务器连接断开触发重连" + DateTime.Now.ToLongTimeString());
            context.WriteAndFlushAsync(this.initialMessage);
            base.ChannelInactive(context);

        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            Console.WriteLine("Exception: " + exception);
            context.CloseAsync();
        }

        /// <summary>
        /// 心跳
        /// </summary>
        /// <param name="context"></param>
        /// <param name="evt"></param>
        public override void UserEventTriggered(IChannelHandlerContext context, object evt)
        {
            if (evt.GetType() == typeof(IdleStateEvent))
            {
                var idleEvent = evt as IdleStateEvent;
                if (idleEvent.State == IdleState.ReaderIdle)
                {
                    int msgType = 3;
                    int bodyLength = 0;
                    IByteBuffer byteBuffer = Unpooled.Buffer(12);
                    byteBuffer.WriteInt(msgType);
                    byteBuffer.WriteInt(bodyLength);
                    byteBuffer.WriteInt(GenerateCheckSum(byteBuffer, 8));

                    var future = context.WriteAndFlushAsync(byteBuffer);
                    if (future.IsCompletedSuccessfully)
                    {
                        Console.WriteLine("Reader心跳发送成功" + DateTime.Now.ToLongTimeString());
                    }
                    else
                    {
                        Console.WriteLine("Reader心跳发送失败" + DateTime.Now.ToLongTimeString());
                        context.Channel.CloseAsync();
                    }
                }
                else if (idleEvent.State == IdleState.WriterIdle)
                {
                    int msgType = 3;
                    int bodyLength = 0;
                    IByteBuffer byteBuffer = Unpooled.Buffer(12);
                    byteBuffer.WriteInt(msgType);
                    byteBuffer.WriteInt(bodyLength);
                    byteBuffer.WriteInt(GenerateCheckSum(byteBuffer, 8));

                    var future = context.WriteAndFlushAsync(byteBuffer);
                    if (future.IsCompletedSuccessfully)
                    {
                        Console.WriteLine("Writer心跳发送成功" + DateTime.Now.ToLongTimeString());
                    }
                    else
                    {
                        Console.WriteLine("Writer心跳发送失败" + DateTime.Now.ToLongTimeString());
                        context.Channel.CloseAsync();
                    }

                }
                else
                {
                    Console.WriteLine("AllIdle:" + DateTime.Now.ToLongTimeString());
                    context.Channel.CloseAsync();
                    //base.UserEventTriggered(context, evt);
                }
            }
        }



        /// <summary>
        /// 获取无锁队列多少数据
        /// </summary>
        /// <param name="obj"></param>
        private static void GetlockFreeCount()
        {
            while (true)
            {
                Console.WriteLine(DateTime.Now.ToLongTimeString() + " 当前队列内数据Count：" + lockFreeQueue.Count);
                Thread.Sleep(60 * 1000);
            }
        }

    }
}