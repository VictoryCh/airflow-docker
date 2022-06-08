select to_char(now(), 'yyyymmdd')   as "date",
       to_char(now(), 'hh24:mi:ss') as "time",
       WERKS,
       BTRTL,
       objid                        as ORGEH,
       LNAME,
       orgname                      as SNAME,
       shortname                    as ABBRV,
       OPRNT,
       chperoe                      as CHPER,
       orglow                       as ZTYPE,
       orgendda                     as SENDD
from organizational_unit_actual_f;

--
-- SELECT
--     hrp1000.objid,
--     hrp1000.del_flag as delflag,
--     hrp1000_p.mc_stext as orgname,
--     hrp1000_p.begda as orgbegda,
--     hrp1000_p.endda as orgendda,
--     hrt1222.low as orglow,
-- 	hrp1008.persa as werks,
-- 	hrp1008.btrtl as btrtl,
-- 	hrp1002.lname as lname,
-- 	hrp1000_p.mc_short as shortname,
-- 	HRP1001_S.OBJID as oprnt,
-- 	HRP1001_L.SOBID as chperoe
--
--    FROM asup.hrp1000 hrp1000
--
--        LEFT JOIN asup.hrp1000_p hrp1000_p ON hrp1000_p.objid = hrp1000.objid AND hrp1000_p.otype = hrp1000.otype AND hrp1000_p.begda <= '2022-05-06' AND hrp1000_p.endda >=  '2022-05-06'
--        LEFT JOIN asup.hrp1222 hrp1222 ON hrp1222.objid = hrp1000.objid AND hrp1222.otype = hrp1000.otype AND hrp1222.begda <=  '2022-05-06' AND hrp1222.endda >=  '2022-05-06'
--        LEFT JOIN asup.hrt1222 hrt1222 ON hrt1222.tabnr = hrp1222.tabnr AND hrt1222.attrib = 'Z_ORG_TYPE'
--
-- 	  LEFT JOIN asup.hrp1008 hrp1008 ON hrp1008.plvar = hrp1000.plvar AND hrp1008.otype = hrp1000.otype AND hrp1008.objid = hrp1000.objid
-- 	   LEFT JOIN asup.hrp1002 ON hrp1002.plvar =  hrp1000.plvar   AND hrp1002.otype =  hrp1000.otype  AND  hrp1002.objid =  hrp1000.objid
-- 	   LEFT JOIN asup.hrp1001_S ON hrp1001_S.plvar = hrp1000.plvar AND hrp1001_S.otype = hrp1000.otype AND  hrp1001_S.sobid = hrp1000.objid
-- 	   LEFT JOIN asup.hrp1001_r ON hrp1001_r.objid = hrp1000.objid
-- 	   LEFT JOIN asup.hrp1001_l ON hrp1001_L.plvar = hrp1000.plvar AND hrp1001_L.otype = hrp1000.otype AND hrp1000.objid = hrp1001_r.objid
--
--
--         where hrp1000.otype = 'O' AND hrp1000_p.begda <=  '2022-05-06' AND hrp1000_p.endda >=  '2022-05-06'
-- 		AND hrp1002.begda <= '2022-05-06' AND hrp1002.endda >= '2022-05-06' AND hrp1001_s.RSIGN = 'В'
-- 		AND hrp1001_r.relat = '002' AND hrp1001_s.sclas = 'O' AND hrp1001_r.otype = 'O' AND hrp1001_r.RSIGN = 'В'
-- 		AND hrp1001_r.relat = '012' AND hrp1001_r.sclas = 'S'
-- 		AND hrp1001_r.objid = hrp1001_l.objid AND hrp1001_l.otype = 'S' AND hrp1001_l.RSIGN = 'A'
-- 		AND  hrp1001_l.relat = '008' AND  hrp1001_l.sclas = 'P' ;
