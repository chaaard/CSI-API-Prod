using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AdjustmentAddDto
    {
        public int? Id { get; set; }
        public string? DisputeReferenceNumber { get; set; } = string.Empty;
        public decimal? DisputeAmount { get; set; }
        public DateTime? DateDisputeFiled { get; set; }
        public string? DescriptionOfDispute { get; set; } = string.Empty;
        public string? NewJO { get; set; } = string.Empty;
        public string? CustomerId { get; set; } = string.Empty;
        public DateTime? AccountsPaymentDate { get; set; }
        public string? AccountsPaymentTransNo { get; set; } = string.Empty;
        public decimal? AccountsPaymentAmount { get; set; }
        public int? ReasonId { get; set; }
        public string? Descriptions { get; set; } = string.Empty;
        public DateTime ModifiedDate { get; set; }
        public bool? DeleteFlag { get; set; }
    }
}
