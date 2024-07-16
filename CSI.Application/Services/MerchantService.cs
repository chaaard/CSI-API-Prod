using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;

namespace CSI.Application.Services
{
    public class MerchantService : IMerchantService
    {
        private readonly AppDBContext _dbContext;

        public MerchantService(AppDBContext dBContext)
        {
            _dbContext = dBContext;
        }

        public async Task<List<Merchant>> GetMerchant()
        {
            var getMerchant = new List<Merchant>();
            getMerchant = await _dbContext.Merchant
                .FromSqlRaw($"SELECT " +
                $"CUCSN[MerchantCode], " +
                $"CUNMFL[MerchantName], " +
                $"CACSAN[MerchantNo] " +
                $"FROM OPENQUERY([SNR],'SELECT " +
                $"A.CUCSN, " +
                $"A.CUNMFL, " +
                $"B.CACSAN " +
                $"FROM MMJDALIB.CIMCUS A " +
                $"INNER JOIN MMJDALIB.CRMCSA B " +
                $"ON A.CUCSN = B.CACSN');")
                .ToListAsync();
            return getMerchant;
        }
    }
}
