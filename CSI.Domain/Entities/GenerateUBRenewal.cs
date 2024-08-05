using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class GenerateUBRenewal
    {
        public string? Ids { get; set; } = string.Empty;
        public int? LocationId { get; set; }
        public DateTime? AutoChargeDate { get; set; } 
        public int? Gold { get; set; }
        public decimal? Amount700 { get; set; }
        public int? Business { get; set; }
        public decimal? Amount900 { get; set; }
        public int? AddOnFree { get; set; }
        public decimal? TotalAmount { get; set; }
        public string? CSINo { get; set; } = string.Empty;
        public DateTime? TransactedDate { get; set; }
    }
}
