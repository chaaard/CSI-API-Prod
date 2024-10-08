﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AnalyticsDto
    {
        public int Id { get; set; }
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
        public decimal? SubTotal { get; set; }
        public Guid? UserId { get; set; }
        public int StatusId { get; set; }
        public bool? IsUpload { get; set; }
        public bool? IsGenerate { get; set; }
        public bool? IsTransfer { get; set; }
        public bool? DeleteFlag { get; set; }
        public string? InvoiceNo { get; set; } = string.Empty;
        public int? TrxCount { get; set; }
        public string? Remarks { get; set; } = string.Empty;
        public int? Sequence { get; set; }

    }
}
