using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class Merchant
    {
        public decimal? MerchantCode { get; set; }
        public string? MerchantName { get; set; } = string.Empty;
        public string? MerchantNo { get; set; } = string.Empty;
    }
}
