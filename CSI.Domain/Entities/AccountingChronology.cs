﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingChronology
    {
        public int Id { get; set; }
        public int MatchId { get; set; }
        public int AdjustmentId { get; set; }
        public string? CustomerId { get; set; } = string.Empty;
        public DateTime? TransactionDate { get; set; } = null;
        public string? OrderNo { get; set; } = string.Empty;
        public decimal? NonMembershipFee { get; set; }
        public decimal? PurchasedAmount { get; set; }
        public decimal? Amount { get; set; }
        public int? StatusId { get; set; }
        public int? StoreId { get; set; }
        public decimal? AgencyFee { get; set; }
        public int? FileDescriptionId { get; set; }
        public bool DeleteFlag { get; set; }
    }
}
