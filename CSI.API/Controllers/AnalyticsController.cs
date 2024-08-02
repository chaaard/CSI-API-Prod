using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Application.Services;
using CSI.Domain.Entities;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Cors;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace CSI.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    [EnableCors("AllowOrigin")]
    //[Authorize]
    public class AnalyticsController : ControllerBase
    {
        public readonly IAnalyticsService _analyticsService;

        public AnalyticsController(IAnalyticsService analyticsService)
        {
            _analyticsService = analyticsService;
        }

        [HttpPost("GetAnalytics")]
        public async Task<IActionResult> GetAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetAnalytics(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("GetAnalyticsUB")]
        public async Task<IActionResult> GetAnalyticsUB(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetAnalyticsUB(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("GetAnalyticsByItem")]
        public async Task<IActionResult> GetAnalyticsByItem(RefreshAnalyticsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetAnalyticsByItem(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("GenerateUBVoucher")]
        public async Task<IActionResult> GenerateUBVoucher(RefreshAnalyticsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GenerateUBVoucher(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }
        [HttpPost("GenerateUBRenewal")]
        public async Task<IActionResult> GenerateUBRenewal(RefreshAnalyticsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GenerateUBRenewal(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("GetAnalyticsProofListVariance")]
        public async Task<IActionResult> GetAnalyticsProofListVariance(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetAnalyticsProofListVariance(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("GetTotalAmountPerMechant")]
        public async Task<IActionResult> GetTotalAmountPerMechant(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetTotalAmountPerMerchant(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("SaveException")]
        public async Task<IActionResult> SaveException(AnalyticsProoflistDto refreshAnalyticsDto)
        {
            int result = await _analyticsService.SaveException(refreshAnalyticsDto);
            if (result != 0)
            {
                return Ok(result);
            }

            return NotFound();
        }

        [HttpPost("RefreshAnalytics")]
        public async Task RefreshAnalytics(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            await _analyticsService.RefreshAnalytics(refreshAnalyticsDto);
        }



        [HttpPost("SubmitAnalytics")]
        public async Task<IActionResult> SubmitAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.SubmitAnalytics(analyticsParamsDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("SubmitAnalyticsWOProoflist")]
        public async Task<IActionResult> SubmitAnalyticsWOProoflist(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.SubmitAnalyticsWOProoflist(analyticsParamsDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GenerateInvoiceAnalytics")]
        public async Task<IActionResult> GenerateInvoiceAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.GenerateInvoiceAnalytics(analyticsParamsDto);

            if (result.Item1 != null)
            {
                var data = new
                {
                    InvoiceList = result.Item1,
                    IsPending = result.Item2
                };

                return (Ok(data));
            }
            return (NotFound());
        }

        [HttpPost("IsSubmittedGenerated")]
        public async Task<IActionResult> IsSubmitted(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.IsSubmittedGenerated(analyticsParamsDto);
            var data = new
            {
                IsSubmitted = result.Item1,
                IsGenerated = result.Item2
            };
            return Ok(data); 
        }

        [HttpPost("UpdateUploadStatus")]
        public async Task<IActionResult> UpdateUploadStatus(AnalyticsParamsDto analyticsParamsDto)
        {
            await _analyticsService.UpdateUploadStatus(analyticsParamsDto);
            return Ok();
        }

        [HttpPost("GenerateWeeklyReport")]
        public async Task<IActionResult> GenerateWeeklyReport(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.GenerateWeeklyReport(analyticsParamsDto);

            if (result.Item1 != null)
            {
                var data = new
                {
                    WeeklyReport = result.Item1,
                    RecapSummary = result.Item2
                };

                return (Ok(data));
            }

            return Ok(result);
        }

        [HttpPost("GetLocations")]
        public async Task<IActionResult> GetLocations()
        {
            var result = await _analyticsService.GetLocations();

            var formatted = result
                .Where(x => x.LocationName.ToLower().Contains("kareila"))
                .Select(x =>
                {
                    // Replace "kareila" with a blank and trim the string
                    x.LocationName = x.LocationName.Replace("KAREILA - ", "").Trim();
                    return x;
                })
                .ToList();


            if (formatted.Any())
            {
                return Ok(formatted);
            }

            return (NotFound());
        }

        [HttpPost("GetClubs")]
        public async Task<IActionResult> GetClubs()
        {
            var result = await _analyticsService.GetClubs();

            if (result != null)
            {
                return (Ok(result));
            }

            return Ok(result);
        }

        [HttpPost("GetGeneratedInvoice")]
        public async Task<IActionResult> GetGeneratedInvoice(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.GetGeneratedInvoice(analyticsParamsDto);

            if (result != null)
            {
                return (Ok(result));
            }

            return Ok(result);
        }

        [HttpPost("GetAnalyticsToDelete")]
        public async Task<IActionResult> GetAnalyticsToDelete(AnalyticsToDeleteDto analyticsToDelete)
        {
            var result = await _analyticsService.GetAnalyticsToDelete(analyticsToDelete);

            if (result.Item1 != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPut("DeleteAnalytics")]
        public async Task<IActionResult> DeleteAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var result = await _analyticsService.DeleteAnalytics(updateAnalyticsDto);
            return (Ok(result));
        }

        [HttpPut("RevertAnalytics")]
        public async Task<IActionResult> RevertAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var result = await _analyticsService.RevertAnalytics(updateAnalyticsDto);
            return (Ok(result));
        }
        [HttpPut("UpdateRemarkInvoice")]
        public async Task<IActionResult> UpdateRemarkInvoice(UpdateGenerateInvoiceDto updateGenerateInvoiceDto)
        {
            var result = await _analyticsService.UpdateRemarkInvoice(updateGenerateInvoiceDto);
            return (Ok(result));
        }
        [HttpPut("CreateUpdateAnalyticsRemarks")]
        public async Task<IActionResult> CreateUpdateAnalyticsRemarks(UpdateGenerateInvoiceDto updateGenerateInvoiceDto)
        {
            var result = await _analyticsService.CreateUpdateAnalyticsRemarks(updateGenerateInvoiceDto);
            return (Ok(result));
        }

        [HttpPut("UpdateAnalytics")]
        public async Task<IActionResult> UpdateAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var result = await _analyticsService.UpdateAnalytics(updateAnalyticsDto);
            return (Ok(result));
        }

        [HttpGet("CheckFolderPath")]
        public IActionResult CheckFolderPath(string path)
        {
            var result = _analyticsService.CheckFolderPath(path);
            return (Ok(result));
        }

        [HttpPost("GenerateA0File")]
        public async Task<IActionResult> GenerateA0File(GenerateA0FileDto generateA0FileDto)
        {
            var result = await _analyticsService.GenerateA0File(generateA0FileDto);

            if (result.Item1 != null)
            {
                var data = new
                {
                    Message = result.Item1,
                    FileName = result.Item2,
                    Content = result.Item3,
                };

                return (Ok(data));
            }
            return (NotFound());
        }

        [HttpPost("ManualReload")]
        public async Task ManualReload(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            //await _analyticsService.ManualReload(refreshAnalyticsDto);
        }

        [HttpPost("GetAnalyticsToUndoSubmit")]
        public async Task<IActionResult> GetAnalyticsToUndoSubmit(AnalyticsUndoSubmitDto analyticsUndoSubmit)
        {
            var result = await _analyticsService.GetAnalyticsToUndoSubmit(analyticsUndoSubmit);

            if (result.Item1 != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("UndoSubmitAnalytics")]
        public async Task<IActionResult> UndoSubmitAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.UndoSubmitAnalytics(analyticsParamsDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("AccountingGenerateInvoice")]
        public async Task<IActionResult> AccountingGenerateInvoice(GenerateA0FileDto generateA0FileDto)
        {
            var result = await _analyticsService.AccountingGenerateInvoice(generateA0FileDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("DashboardAccounting")]
        public async Task<IActionResult> DashboardAccounting(GenerateA0FileDto generateA0FileDto)
        {
            var result = await _analyticsService.DashboardAccounting(generateA0FileDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("FileDescriptions")]
        public async Task<IActionResult> FileDescriptions()
        {
            var result = await _analyticsService.FileDescriptions();

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetAccountingProoflist")]
        public async Task<IActionResult> GetAccountingProoflist(PaginationDto paginationDto)
        {
            var result = await _analyticsService.GetAccountingProoflist(paginationDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetAccountingAnalyitcs")]
        public async Task<IActionResult> GetAccountingAnalyitcs(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetAccountingAnalyitcs(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("GetAccountingProofListVariance")]
        public async Task<IActionResult> GetAccountingProofListVariance(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetAccountingProofListVariance(analyticsParamsDto);

                if (result.Item1.Count() >= 1)
                {
                    return (Ok(result));
                }

                return NotFound(result);
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("CreateAnalytics")]
        public async Task<IActionResult> CreateAnalytics(AnalyticsAddDto analyticsAddDto)
        {
            // var result = await _analyticsService.CreateAnalytics(analyticsAddDto);
            await _analyticsService.CreateAnalytics(analyticsAddDto);

            // if(result != null) BadRequest();

            return Ok("Successfully Created");
        }

        [HttpPost("ExportExceptions")]
        public async Task<IActionResult> ExportExceptions(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            var result = await _analyticsService.ExportExceptions(refreshAnalyticsDto);

            if (result != null)
            {
                return (Ok(result));
            }

            return Ok(result);
        }

        [HttpPost("InsertLogs")]
        public async Task<IActionResult> InsertLogs(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            _analyticsService.InsertLogs(refreshAnalyticsDto);
            return Ok();
        }

        [HttpPost("GetVarianceMMS")]
        public async Task<IActionResult> GetVarianceMMS(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            var data = await _analyticsService.GetVarianceMMS(refreshAnalyticsDto);
            // Check if result is empty
            if (data.Count == 0)
            {
                // If no records are returned, create a new list with default values
                data = new List<VarianceMMS>
                {
                    new VarianceMMS { MMS = 0, CSI = 0, Variance = 0 }
                };
            }
            else
            {
                data = data.Select(m => new VarianceMMS
                {
                    MMS = m.MMS,
                    CSI = m.CSI,
                    Variance = m.Variance
                }).ToList();
            }
            return Ok(data);
        }
        [HttpPost("GetVarianceMMSPerMerchant")]
        public async Task<IActionResult> GetVarianceMMSPerMerchant(RefreshAnalyticsDto analyticsParamsDto)
        {
            try
            {
                var result = await _analyticsService.GetVarianceMMSPerMerchant(analyticsParamsDto);

                if (result != null)
                {
                    return Ok(result);
                }

                return NotFound();
            }
            catch (OperationCanceledException)
            {
                // Handle cancellation if needed
                return StatusCode(499, "Request canceled"); // 499 Client Closed Request is a common status code for cancellation
            }
            catch (Exception ex)
            {
                // Handle other exceptions
                return StatusCode(500, $"Internal Server Error: {ex.Message}");
            }
        }

        [HttpPost("UpdateAccountingAdjustments")]
        public async Task<IActionResult> UpdateAccountingAdjustments(AccountingAdjustmentDto accountingAdjustmentDto)
        {
            var result = await _analyticsService.UpdateAccountingAdjustments(accountingAdjustmentDto);
            return (Ok(result));
        }

        [HttpPost("GetAccountingPaymentProofList")]
        public async Task<IActionResult> GetAccountingPaymentProofList(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.GetAccountingPaymentProofList(analyticsParamsDto);

            if (result != null)
            {
                return (Ok(result));
            }

            return Ok(result);
        }

        [HttpGet("GetAccountingAdjustments")]
        public async Task<IActionResult> GetAccountingAdjustments(int Id)
        {
            var result = await _analyticsService.GetAdjustments(Id);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetAccountingProoflistAdjustments")]
        public async Task<IActionResult> GetAccountingProoflistAdjustments(PaginationDto paginationDto)
        {
            var result = await _analyticsService.GetAccountingProoflistAdjustments(paginationDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetHistoryPaymentRecon")]
        public async Task<IActionResult> GetHistoryPaymentRecon(int id)
        {
            var result = await _analyticsService.GetHistoryPaymentRecon(id);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }

        [HttpPost("GetBalancesDetails")]
        public async Task<IActionResult> GetBalancesDetails(AnalyticsParamsDto analyticsParamsDto)
        {
            var result = await _analyticsService.GetBalancesDetails(analyticsParamsDto);

            if (result != null)
            {
                return (Ok(result));
            }
            return (NotFound());
        }
    }
}
