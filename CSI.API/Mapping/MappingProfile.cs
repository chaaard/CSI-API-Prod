using AutoMapper;
using CSI.Application.DTOs;
using CSI.Domain.Entities;

namespace CSI.API.Mapping
{
    public class MappingProfile : Profile
    {
        public MappingProfile()
        {
            CreateMap<User, UserDto>();
            CreateMap<AdjustmentAddDto, Adjustments>();
            CreateMap<AnalyticsProoflistDto, AnalyticsProoflist>();
            CreateMap<AnalyticsDto, Analytics>(); 

            CreateMap<AnalyticsAddDto, Analytics>()
                .ForMember(x => x.CustomerId, o => o.MapFrom(s => s.Merchant))
                .ForMember(x => x.LocationId, o => o.MapFrom(s => s.Club)); 

            CreateMap<GenerateInvoiceDto, GenerateInvoice>();
            CreateMap<LogsDto, Logs>();
        }
    }
}
