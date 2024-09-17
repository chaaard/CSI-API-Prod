using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class CMTranDto
    {
        public decimal CSDATE {get;set;}
        public decimal CSSTOR {get;set;}
        public decimal CSREG  {get;set;}
        public decimal CSTRAN {get;set;}
        public decimal CSCUST {get;set;}
        public decimal CSTAMT {get;set;}
        public string? CSTDOC {get;set;}
        public string? CSCARD {get;set;}
        public string? CSDTYP {get;set;}
        public decimal CSTIL  {get;set;}
        public decimal CSSEQ  { get; set; }

        //public decimal CSDATE { get; set; }
        //public decimal CSSTOR { get; set; }
        //public decimal CSREG { get; set; } //RegisterNo
        //public decimal CSTRAN { get; set; }  //TRX No
        //public decimal CSCUST { get; set; } //Membership No
        //public decimal CSTAMT { get; set; } //Amount
        //public string? CSTDOC { get; set; } = string.Empty; //Customer Code
        //public string? CSCARD { get; set; } = string.Empty; //Job Order No 
        //public string CSDTYP { get; set; } = string.Empty;
        //public decimal CSTIL { get; set; }
        //public decimal CSSEQ { get; set; }

    }
}
