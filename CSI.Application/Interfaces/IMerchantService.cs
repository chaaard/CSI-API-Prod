using CSI.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Interfaces
{
    public interface IMerchantService
    {
        Task<List<Merchant>> GetMerchant();
        Task<List<Merchant>> GetAllMerchant();
    }
}
