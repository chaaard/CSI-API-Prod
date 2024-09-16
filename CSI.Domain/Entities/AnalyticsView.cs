using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AnalyticsView
    {
        public int Id { get; set; }
        public int LocationId { get; set; }
        public string? CustomerId { get; set; } = string.Empty;
        public string? CustomerName { get; set; } = string.Empty;
        public string? LocationName { get; set; }
        public DateTime? TransactionDate { get; set; }
        public string? MembershipNo { get; set; } = string.Empty;
        public string? CashierNo { get; set; } = string.Empty;
        public string? RegisterNo { get; set; } = string.Empty;
        public string? TransactionNo { get; set; } = string.Empty;
        public string? OrderNo { get; set; } = string.Empty;
        public int? Qty { get; set; }
        public decimal? Amount { get; set; }
        public decimal SubTotal { get; set; }
        public int StatusId { get; set; }
        public int? DeleteFlag { get; set; }
        public int? IsUpload { get; set; }
        public int? IsGenerate { get; set; }
        public int? IsTransfer { get; set; }
        public string? Remarks { get; set; } = string.Empty;
        public int? Sequence { get; set; }
        public string? InvoiceNo { get; set; } = string.Empty;
    }
}
