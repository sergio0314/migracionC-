using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_Ele_PPYE
    {
        public string IdCategoria { get; set; }
        public string DocumentoContable { get; set; }
        public string CodCategoria { get; set; }
        public DateTime FechaCompra { get; set; }
        public string IdSucursal { get; set; }
        public string IdProveedor { get; set; }
        public string Elemento { get; set; }
        public string Ref1 { get; set; }
        public string Ref2 { get; set; }
        public string Factura { get; set; }
        public string IdGrupoContable { get; set; }
        public string IdCuenta { get; set; }
        public string CodAgrupacion { get; set; }
        public DateTime FechaCreacion { get; set; }
        public string IdUsuario { get; set; }
        public string ConsCodigo { get; set; }


        //para la consulta
        public string scat_ppe { get; set; }
        public string sbcat_ppe { get; set; }
    }
}
