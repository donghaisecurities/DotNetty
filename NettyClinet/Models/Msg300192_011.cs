using System;
using System.Collections.Generic;
using System.Text;

namespace NettyClinet
{
    /// <summary>
    /// 逐笔委托
    /// </summary>
    public struct Msg300192_011
    {
        public int ChannelNo;
        public long ApplSeqNum;
        public string MdStreamID;
        public string SecurityID;
        public string SecurityIDSource;
        public double Price;
        public double OrderQty;
        public string Side;
        public long TransactTime;
    }
}
