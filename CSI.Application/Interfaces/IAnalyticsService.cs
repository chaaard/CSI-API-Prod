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
        Task<List<MatchDto>> GetAnalyticsProofListVariance(AnalyticsParamsDto analyticsParamsDto);
        Task<Dictionary<string, decimal?>> GetTotalAmountPerMerchant(AnalyticsParamsDto analyticsParamsDto);
        Task RefreshAnalytics(RefreshAnalyticsDto analyticsParam);
        Task<bool> SubmitAnalytics(AnalyticsParamsDto analyticsParamsDto);
        Task<(List<InvoiceDto>, bool)> GenerateInvoiceAnalytics(AnalyticsParamsDto analyticsParamsDto);
        Task<(bool, bool)> IsSubmittedGenerated(AnalyticsParamsDto analyticsParamsDto);
        Task UpdateUploadStatus(AnalyticsParamsDto analyticsParamsDto);
        Task<(List<WeeklyReportDto>, List<RecapSummaryDto>)> GenerateWeeklyReport(AnalyticsParamsDto analyticsParamsDto);
        Task<List<Location>> GetLocations();
        Task<List<GenerateInvoice>> GetGeneratedInvoice(AnalyticsParamsDto analyticsParamsDto);
        Task<List<int>> GetClubs();
        Task<(List<AnalyticsDto>, int)> GetAnalyticsToDelete(AnalyticsToDeleteDto analyticsToDelete);
        Task<bool> DeleteAnalytics(int id);
        Task<bool> UpdateAnalytics(UpdateAnalyticsDto updateAnalyticsDto);
        bool CheckFolderPath(string path);
        Task<(string, string, string)> GenerateA0File(GenerateA0FileDto generateA0FileDto);
        Task ManualReload(RefreshAnalyticsDto analyticsParam);
        Task<bool> RevertAnalytics(int id);
        Task<(List<AnalyticsDto>, int)> GetAnalyticsToUndoSubmit(AnalyticsUndoSubmitDto analyticsUndoSubmit);
        Task<bool> UndoSubmitAnalytics(AnalyticsParamsDto analyticsParamsDto);
        Task<List<AccntGenerateInvoiceDto>> AccountingGenerateInvoice(GenerateA0FileDto generateA0FileDto);
        Task<List<DashboardAccounting>> DashboardAccounting(GenerateA0FileDto generateA0FileDto);
    }
}
