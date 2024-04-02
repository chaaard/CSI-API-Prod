using CSI.Application.DTOs;
using CSI.Domain.Entities;
using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Interfaces
{
    public interface IProofListService
    {
        Task<(List<Prooflist>?, string?)> ReadProofList(List<IFormFile> files, string customerName, string strClub, string selectedDate, string analyticsParamsDto);
        Task<List<PortalDto>> GetPortal(PortalParamsDto portalParamsDto);
    }
}
