using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AccountingMatchPaymentDto
    {
        public int? MatchId { get; set; }
        public int? AnalyticsId { get; set; }
        public int? ProofListId { get; set; }
        public string? Status { get; set; } = string.Empty;
        public DateTime? TransactionDate { get; set; }
        public string? OrderNo { get; set; }
        public decimal? ProofListAmount { get; set; }
        public decimal? AnalyticsAmount { get; set; }
        public decimal? Variance { get; set; }
        public string? Location { get; set; } = string.Empty;
    }
}
