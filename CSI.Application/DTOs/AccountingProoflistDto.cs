using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AccountingProoflistDto
    {
        public int Id { get; set; }
        public string? CustomerId { get; set; } = string.Empty;
        public DateTime? TransactionDate { get; set; } = null;
        public string? OrderNo { get; set; } = string.Empty;
        public decimal? NonMembershipFee { get; set; }
        public decimal? PurchasedAmount { get; set; }
        public decimal? Amount { get; set; }
        public int? Status { get; set; }
        public decimal? GrossCommission { get; set; }
        public decimal? NetOfVAT { get; set; }
        public decimal? TwelvePercentInputVAT { get; set; }
        public decimal? EWT { get; set; }
        public decimal? NetPaid { get; set; }
        public string? StoreName { get; set; } = string.Empty;
        public int? FileDescriptionId { get; set; }
        public bool DeleteFlag { get; set; }
    }
}
