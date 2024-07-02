using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingMatchPayment
    {
        public int Id { get; set; }
        public int? AccountingAnalyticsId { get; set; }
        public int? AccountingProofListId { get; set; }
        public int? AccountingStatusId { get; set; }
        public int? AccountingAdjustmentId { get; set; }
        public bool DeleteFlag { get; set; }
    }
}
