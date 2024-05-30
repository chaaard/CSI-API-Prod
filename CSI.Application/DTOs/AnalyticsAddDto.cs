using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AnalyticsAddDto
    {
        [Required]
        public string? Merchant { get; set; } = string.Empty;
        [Required]
        public int? Club { get; set; }
        [Required]
        public DateTime? TransactionDate { get; set; }
        [Required]
        public string? MembershipNo { get; set; } = string.Empty;
        [Required]
        public string? CashierNo { get; set; } = string.Empty;
        [Required]
        public string? RegisterNo { get; set; } = string.Empty;
        [Required]
        public string? TransactionNo { get; set; } = string.Empty;
        [Required]
        public string? OrderNo { get; set; } = string.Empty;
        [Required]
        public int? Qty { get; set; }
        [Required]
        public decimal? Amount { get; set; }
        [Required]
        public decimal? SubTotal { get; set; }
        public bool? IsUpload { get; set; } = true;
        public bool? DeleteFlag { get; set; } = false;
    }
}