using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class GenerateUBVoucher
    {
        public int? Id { get; set; }
        public int? LocationId { get; set; }
        public DateTime? TransactionDate { get; set; }
        public string? OrderNo { get; set; } = string.Empty;
        public string? TransactionNo { get; set; } = string.Empty;
        public decimal? SKU { get; set; }
        public string? Description { get; set; } = string.Empty;
        public decimal? SRP { get; set; }
        public decimal? UnionBank { get; set; }
        public decimal? KMC { get; set; }
    }
}
