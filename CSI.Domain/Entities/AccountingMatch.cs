using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingMatch
    {
        public int? AnalyticsId { get; set; }
        public string? AnalyticsPartner { get; set; } = string.Empty;
        public string? AnalyticsInvoiceNo { get; set; } = string.Empty;
        public string? AnalyticsLocation { get; set; } = string.Empty;
        public DateTime? AnalyticsTransactionDate { get; set; }
        public string? AnalyticsOrderNo { get; set; } = string.Empty;
        public decimal? AnalyticsAmount { get; set; }
        public int? ProofListId { get; set; }
        public DateTime? ProofListTransactionDate { get; set; }
        public string? ProofListOrderNo { get; set; } = string.Empty;
        public decimal? ProofListAmount { get; set; }
        public int? IsUpload { get; set; }
    }
}
