using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class CustomerTransactionDto: BaseModel
    {
        public string? CustomerName { get; set; }
        public string? CustomerCode { get; set; }
        public string TransactionDate {  get; set; } = string.Empty;
        public string? MembershipNo { get; set; }
        public string? CashierNo { get; set; }
        public string? RegisterNo { get; set; }
        public string? TransactionNo { get; set; }
        public string? JobOrderNo { get; set; }
        public bool? IsDeleted { get; set; }
    }

    public class CMSearchParams
    {
        public VarianceMMS Variance { get; set; }
        public string? CustomerCode { get; set; }
        public string? JobOrderNo { get; set; }
        public int LocationId { get; set; }
    }
}
