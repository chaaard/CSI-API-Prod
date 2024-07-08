using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingMatch
    {
        public int? MatchId { get; set; }
        public int? AnalyticsId { get; set; }
        public string? AnalyticsInvoiceNo { get; set; } = string.Empty;
        public string? AnalyticsPartner { get; set; } = string.Empty;
        public string? AnalyticsLocation { get; set; } = string.Empty;
        public DateTime? AnalyticsTransactionDate { get; set; }
        public string? AnalyticsOrderNo { get; set; }
        public decimal? AnalyticsAmount { get; set; }
        public int? ProofListId { get; set; }
        public string? ProofListPartner { get; set; } = string.Empty;
        public decimal? ProofListAmount { get; set; }
        public string? ProofListOrderNo { get; set; } = string.Empty;
        public DateTime? ProofListTransactionDate { get; set; }
        public string? ProofListLocation { get; set; } = string.Empty;
        public decimal? ProofListAgencyFee { get; set; }
        public string? Status { get; set; } = string.Empty;
    }
}
