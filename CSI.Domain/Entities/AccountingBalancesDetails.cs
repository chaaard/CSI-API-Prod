using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingBalancesDetails
    {
        public int? MatchId { get; set; }
        public string? OracleInvNo { get; set; } = string.Empty;
        public DateTime? InvoiceDate { get; set; }
        public string? OrderNumber { get; set; } = string.Empty;
        public string? TrxNo { get; set; }
        public string? RegNo { get; set; }
        public int? LocationCode { get; set; }
        public string? OutletName { get; set; }
        public decimal? GROSSPERSNR { get; set; }
        public decimal? GROSSPERMERCHANT { get; set; }
        public decimal? ACCOUNTSPAYMENT { get; set; }
        public decimal? CHARGEABLE { get; set; }
        public string? Status { get; set; }
    }
}
