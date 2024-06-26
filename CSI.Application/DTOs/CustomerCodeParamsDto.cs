﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class CustomerCodeParamsDto
    {
        public int Id { get; set; }
        public string CustomerNo { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
        public string CustomerCode { get; set; } = string.Empty;
        public int CategoryId { get; set; }
        public bool DeleteFlag { get; set; }
        public string? UserId { get; set; } = string.Empty;
    }
}
