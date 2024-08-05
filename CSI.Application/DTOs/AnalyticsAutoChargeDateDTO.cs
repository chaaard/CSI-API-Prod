using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AnalyticsAutoChargeDateDTO
    {
        public DateTime[]? dates { get; set; }
        public string UserId { get; set; } = string.Empty;
        public int[] storeId { get; set; }
        public string Ids { get; set; } = string.Empty;
    }
}
