using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class ExceptionReportDto
    {
        public int Id { get; set; }
        public string? CustomerId { get; set; } = string.Empty;
        public string? JoNumber { get; set; } = string.Empty;
        public DateTime? TransactionDate { get; set; }
        public decimal? Amount { get; set; }
        public string? AdjustmentType { get; set; } = string.Empty;
        public string? Source { get; set; } = string.Empty;
        public string? Status { get; set; } = string.Empty;
        public string? LocationName { get; set; } = string.Empty;
        public string OldJo { get; set; }
        public string? NewJo { get; set; }
        public string OldCustomerId { get; set; }
        public string? NewCustomerId { get; set; }
        public string? DisputeReferenceNumber { get; set; } = string.Empty;
        public decimal? DisputeAmount { get; set; }
        public DateTime? DateDisputeFiled { get; set; }
        public string? DescriptionOfDispute { get; set; } = string.Empty;
        public DateTime? AccountsPaymentDate { get; set; }
        public string? AccountsPaymentTransNo { get; set; } = string.Empty;
        public decimal? AccountsPaymentAmount { get; set; }
        public string? ReasonDesc { get; set; }
        public string? Descriptions { get; set; } = string.Empty;
    }
}
