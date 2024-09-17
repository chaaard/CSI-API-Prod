using CSI.Application.DTOs;
using CSI.Domain.Entities;

namespace CSI.Application.Interfaces
{
    public interface ICreditMemoService
    {
        Task<CreditMemoTranDto> GetCMVariance(VarianceParams variance);
        Task<CreditMemoTranDto> RetrieveUpdateCreditMemoData(VarianceParams variance);
        Task<CreditMemoTranDto> SearchCreditMemoItem(CMSearchParams searchParams);
        bool UpdateCreditMemoStatus(CreditMemoDto custTranList);
        Task<bool> UpdateCustCreditMemo(CustomerTransactionDto custDto);
    }
}