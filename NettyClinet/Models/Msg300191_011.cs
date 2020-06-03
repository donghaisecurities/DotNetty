using System;
using System.Collections.Generic;
using System.Text;

namespace NettyClinet
{
    /// <summary>
    /// 逐笔成交
    /// </summary>
    public struct Msg300191_011
    {
        public int ChannelNo;
        public long ApplSeqNum;
        public string MdStreamID;
        public long BidApplSeqNum;
        public long OfferApplSeqNum;
        public string SecurityID;
        public string SecurityIDSource;
        public double LastPx;
        public double LastQty;
        public string ExecType;
        public long TransactTime;
    }
}
