select to_char(now(), 'yyyymmdd')        as "date",
       to_char(now(), 'hh24:mi:ss')      as "time",
       pdaf.PERNR,
       split_part(pdaf.persname, ' ', 1) as NACHN,
       split_part(pdaf.persname, ' ', 2) as VORNA,
       split_part(pdaf.persname, ' ', 3)    MIDNM,
       ORGEH,
       PLANS,
       paf.posname                       as PLTXT,
       ouaf.chperoe                      as CHPER,
       pdaf.WERKS,
       pdaf.BTRTL,
       pdaf.persgbdat                    as GBDAT,
       case
           when pa2001.sprps != 'X' and (pa2001.subty in (0250, 0251, 0253, 0254) or (pa2001.subty between 0500 and 0520)) then 'Dekret'
           when zactiv.endda is not null then 'Escape'
           when pa2001.persk = 4 then 'GPH'
           when pa2001.sprps != 'X' and pa2001.abwtg >= 3 and pa2001.subty in (0100, 0101, 0102, 0190, 0200, 0201, 0202, 0203, 0204,
                0205, 0206, 0208, 0209, 0210, 0211, 0213, 0214, 0215, 0216, 0217,
                0218, 0219, 0220, 0221, 0222, 0230, 0250, 0251, 0253, 0254, 0260,
                0261, 0280, 0281, 0282, 0283, 0284, 0286, 0287, 0288, 0385, 0390,
                0410, 0600, 0601, 0602, 0606) then 'Аbsence'
           else 'OnWork'
       end PSTAT,
       pdaf.PERSG, pdaf.PERSK, pdaf.persendda as FDATE, pdaf.persbegda as HDATE
from personal_data_actual_f pdaf
left join position_actual_f paf on paf.plansorgid = pdaf."plans"
left join organizational_unit_actual_f ouaf on ouaf.objid = pdaf.orgeh
left join pa2001 p on pa2001.pernr = pdaf.pernr;