using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AccountingAdjustmentDto
    {
        public int Id { get; set; }
        public int? AccountingAdjustmentTypeId { get; set; }
        public DateTime? NewTransactionDate { get; set; }
        public string? AccountPaymentReferenceNo { get; set; }
        public decimal? Amount { get; set; }
        public string Remarks { get; set; }
        public int? MatchId { get; set; }
        public int? ProofListMatchId { get; set; }
        public int? AccountingAnalyticsId { get; set; }
        public int? AccountingProofListId { get; set; }
        public bool? DeleteFlag { get; set; }
        public AnalyticsParamsDto? analyticsParamsDto { get; set; }
    }
}
