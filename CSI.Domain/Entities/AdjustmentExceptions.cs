using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AdjustmentExceptions
    {
        public int Id { get; set; }
        public string? CustomerName { get; set; } = string.Empty;
        public string? OrderNo { get; set; } = string.Empty;
        public DateTime? TransactionDate { get; set; }
        public decimal? SubTotal { get; set; }
        public string? Action { get; set; } = string.Empty;
        public string? SourceType { get; set; } = string.Empty;
        public string? StatusName { get; set; } = string.Empty;
        public int AdjustmentId { get; set; }
        public string? LocationName { get; set; } = string.Empty;
        public int? AnalyticsId { get; set; }
        public int? ProoflistId { get; set; }
        public string OldJO { get; set; } = string.Empty;
        public string? NewJO { get; set; }
        public string CustomerIdOld { get; set; } = string.Empty;
        public string? CustomerIdNew { get; set; } = null;
        public string? DisputeReferenceNumber { get; set; } = string.Empty;
        public decimal? DisputeAmount { get; set; }
        public DateTime? DateDisputeFiled { get; set; }
        public string? DescriptionOfDispute { get; set; } = string.Empty;
        public DateTime? AccountsPaymentDate { get; set; }
        public string? AccountsPaymentTransNo { get; set; } = string.Empty;
        public decimal? AccountsPaymentAmount { get; set; }
        public int? ReasonId { get; set; }
        public string? ReasonDesc { get; set; }
        public string? Descriptions { get; set; } = string.Empty;
    }
}
