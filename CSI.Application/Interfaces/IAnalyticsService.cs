using CSI.Application.DTOs;
using CSI.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Interfaces
{
    public interface IAnalyticsService
    {
        Task<List<AnalyticsDto>> GetAnalytics(AnalyticsParamsDto analyticsParamsDto);
        Task<List<AnalyticsDto>> GetAnalyticsUB(AnalyticsParamsDto analyticsParamsDto);
        Task<List<MatchDto>> GetAnalyticsProofListVariance(AnalyticsParamsDto analyticsParamsDto);
        Task<List<AnalyticsSearchDto>> GetAnalyticsByItem(RefreshAnalyticsDto analyticsParam);
        Task<List<GenerateUBVoucherDto>> GenerateUBVoucher(RefreshAnalyticsDto analyticsParam);
        Task<List<GenerateUBRenewalDto>> GenerateUBRenewal(RefreshAnalyticsDto analyticsParam);
        Task<Dictionary<string, decimal?>> GetTotalAmountPerMerchant(AnalyticsParamsDto analyticsParamsDto);
        Task RefreshAnalytics(RefreshAnalyticsDto analyticsParam);
        Task<int> SaveException(AnalyticsProoflistDto analyticsParam);
        Task<bool> SubmitAnalytics(AnalyticsParamsDto analyticsParamsDto);
        Task<bool> SubmitAnalyticsWOProoflist(AnalyticsParamsDto analyticsParamsDto);
        Task<(List<InvoiceDto>, bool)> GenerateInvoiceAnalytics(AnalyticsParamsDto analyticsParamsDto);
        Task<(bool, bool)> IsSubmittedGenerated(AnalyticsParamsDto analyticsParamsDto);
        Task UpdateUploadStatus(AnalyticsParamsDto analyticsParamsDto);
        Task<(List<WeeklyReportDto>, List<RecapSummaryDto>)> GenerateWeeklyReport(AnalyticsParamsDto analyticsParamsDto);
        Task<List<Location>> GetLocations();
        Task<List<GenerateInvoice>> GetGeneratedInvoice(AnalyticsParamsDto analyticsParamsDto);
        Task<List<int>> GetClubs();
        Task<(List<AnalyticsDto>, int)> GetAnalyticsToDelete(AnalyticsToDeleteDto analyticsToDelete);
        Task<bool> DeleteAnalytics(UpdateAnalyticsDto updateAnalyticsDto);
        Task<bool> UpdateAnalytics(UpdateAnalyticsDto updateAnalyticsDto);
        bool CheckFolderPath(string path);
        Task<(string, string, string)> GenerateA0File(GenerateA0FileDto generateA0FileDto);
        Task ManualReload(RefreshAnalyticsDto analyticsParam);
        Task<bool> RevertAnalytics(UpdateAnalyticsDto updateAnalyticsDto);
        Task<bool> UpdateRemarkInvoice(UpdateGenerateInvoiceDto updateGenerateInvoiceDto);
        Task<bool> CreateUpdateAnalyticsRemarks(UpdateGenerateInvoiceDto updateGenerateInvoiceDto);
        Task<(List<AnalyticsDto>, int)> GetAnalyticsToUndoSubmit(AnalyticsUndoSubmitDto analyticsUndoSubmit);
        Task<bool> UndoSubmitAnalytics(AnalyticsParamsDto analyticsParamsDto);
        Task<List<AccntGenerateInvoiceDto>> AccountingGenerateInvoice(GenerateA0FileDto generateA0FileDto);
        Task<List<DashboardAccounting>> DashboardAccounting(GenerateA0FileDto generateA0FileDto);
        Task<List<FileDescriptions>> FileDescriptions();
        Task<List<AccountingProoflistDto>> GetAccountingProoflist(PaginationDto paginationDto);
        Task<List<AnalyticsDto>> GetAccountingAnalyitcs(AnalyticsParamsDto analyticsParamsDto);
        Task<(List<AccountingMatchDto>, int totalPages)> GetAccountingProofListVariance(AnalyticsParamsDto analyticsParamsDto);
        Task<List<ExceptionReportDto>> ExportExceptions(RefreshAnalyticsDto refreshAnalyticsDto);
        Task<Analytics> CreateAnalytics(AnalyticsAddDto analyticsAddDto);
        Task<List<Logs>> GetLogs();
        void InsertLogs(RefreshAnalyticsDto refreshAnalyticsDto);
        Task<List<VarianceMMS>> GetVarianceMMS(RefreshAnalyticsDto refreshAnalyticsDto);
        Task<List<VarianceMMSCSIDto>> GetVarianceMMSPerMerchant(RefreshAnalyticsDto refreshAnalyticsDto);
        Task<bool> UpdateAccountingAdjustments(AccountingAdjustmentDto accountingAdjustmentDto);
        Task<List<AccountingMatchPaymentDto>> GetAccountingPaymentProofList(AnalyticsParamsDto analyticsParamsDto);
        Task<AccountingAdjustments> GetAdjustments(int Id);
        Task<List<AccountingProoflistAdjustmentsDto>> GetAccountingProoflistAdjustments(PaginationDto paginationDto);
        Task<List<AccountingChronologyDto>> GetHistoryPaymentRecon(int Id);
    }
}
