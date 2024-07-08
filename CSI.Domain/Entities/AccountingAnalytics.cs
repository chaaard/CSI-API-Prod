using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingAnalytics
    {
        public int Id { get; set; }
        public string? CustomerId { get; set; } = string.Empty;
        public int? LocationId { get; set; }
        public DateTime? TransactionDate { get; set; }
        public string? MembershipNo { get; set; } = string.Empty;
        public string? CashierNo { get; set; } = string.Empty;
        public string? RegisterNo { get; set; } = string.Empty;
        public string? TransactionNo { get; set; } = string.Empty;
        public string? OrderNo { get; set; } = string.Empty;
        public int? Qty { get; set; }
        public decimal? Amount { get; set; }
        public decimal? SubTotal { get; set; }
        public string? InvoiceNo { get; set; } = string.Empty;
        public bool DeleteFlag { get; set; }
    }
}
