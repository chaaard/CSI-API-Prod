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
                .FromSqlRaw($"SELECT DISTINCT A.CACSAN[MerchantCode] , A.CUNMFL[MerchantName], CAST(A.CUCSN AS VARCHAR(500)) + ' P'[MerchantNo] FROM OPENQUERY([SNR], 'SELECT A.CUCSN, A.CUNMFL, B.CACSAN FROM MMJDALIB.CIMCUS A INNER JOIN MMJDALIB.CRMCSA B ON A.CUCSN = B.CACSN') AS A WHERE NOT EXISTS (SELECT * FROM tbl_customer AS B WHERE B.CustomerCode = A.CACSAN OR B.CustomerName = A.CUNMFL OR TRY_CONVERT(decimal, B.CustomerCode) = A.CUCSN);")
                .ToListAsync();
            return getMerchant;
        }

        public async Task<List<Merchant>> GetAllMerchant()
        {
            var getMerchant = new List<Merchant>();
            getMerchant = await _dbContext.Merchant
                .FromSqlRaw($"SELECT DISTINCT A.CACSAN[MerchantCode] , A.CUNMFL[MerchantName], CAST(A.CUCSN AS VARCHAR(500)) + ' P'[MerchantNo] FROM OPENQUERY([SNR], 'SELECT A.CUCSN, A.CUNMFL, B.CACSAN FROM MMJDALIB.CIMCUS A INNER JOIN MMJDALIB.CRMCSA B ON A.CUCSN = B.CACSN') AS A;")
                .ToListAsync();
            return getMerchant;
        }
    }
}
