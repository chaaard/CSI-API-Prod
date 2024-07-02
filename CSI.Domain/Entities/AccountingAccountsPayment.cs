using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingAccountsPayment
    {
        public int Id { get; set; }
        public int? MatchId { get; set; }
        public int? AccountingAnalyticsId { get; set; }
        public int? AccountingProofListId { get; set; }
        public string Remarks { get; set; } = string.Empty;
        public string AccountsPaymentRefNo { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public bool DeleteFlag { get; set; }
    }
}
