using CSI.Application.DTOs;
using CSI.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Interfaces
{
    public interface ILocationService
    {
        Task<List<Location>> GetLocation();
        Task<(List<LocationDto>, int totalPages)> GetLocationsAsync(PaginationDto pagination);
        Task<Location> GetLocationByIdAsync(int Id);
        Task<Location> InsertLocationAsync(Location location);
        Task<Location> UpdateLocationByIdAsync(LocationDto location);
        Task<bool> DeleteLocationByIdAsync(int Id);
        Task<List<Location>> GetLocationDdCodesAsync();
    }
}
