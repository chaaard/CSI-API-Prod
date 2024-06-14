using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AnalyticsInsertDto
    {
        public int Id { get; set; }
        public string? CustomerId { get; set; } = string.Empty;
        public string? LocationId { get; set; }
        public DateTime? TransactionDate { get; set; }
        public string? MembershipNo { get; set; } = string.Empty;
        public string? CashierNo { get; set; } = string.Empty;
        public string? RegisterNo { get; set; } = string.Empty;
        public string? TransactionNo { get; set; } = string.Empty;
        public string? OrderNo { get; set; } = string.Empty;
        public int? Qty { get; set; }
        public decimal? Amount { get; set; }
        public decimal? SubTotal { get; set; }
        public Guid? UserId { get; set; }
        public int StatusId { get; set; } = 5;
        public bool? IsUpload { get; set; } = false;
        public bool? IsGenerate { get; set; } = false;
        public bool? IsTransfer { get; set; } = false;
        public bool? DeleteFlag { get; set; } = false;
        public string? InvoiceNo { get; set; } = string.Empty;
    }
}
