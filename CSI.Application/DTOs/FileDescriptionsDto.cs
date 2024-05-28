using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class FileDescriptionsDto
    {
        public int Id { get; set; }
        public string FileName { get; set; } = string.Empty;
        public DateTime UploadDate { get; set; }
        public string Merchant { get; set; } = string.Empty;
        public int Count { get; set; }
        public List<AccountingProoflistDto> AccountingProoflistDtos { get; set; } = new List<AccountingProoflistDto>();
    }
}
