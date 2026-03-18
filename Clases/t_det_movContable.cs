using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_det_movContable
    {
        public long IdMovimiento { get; set; }
        public string Cuenta { get; set; }
        public string NombreCuenta { get; set; }
        public long Base { get; set; }
        public long Factor { get; set; }
        public decimal Debe { get; set; }
        public decimal Haber { get; set; }
        public string Naturaleza { get; set; }
        public long CodigoPPYE { get; set; }
        public string Anexo { get; set; }
        public string DocReferencia { get; set; }
        public DateTime FechaVencimiento { get; set; }
        public string IdCodigoICA { get; set; }
        public string IdVendedor { get; set; }
        public long IdOperacion { get; set; }
        public string IdZona { get; set; }
        public string ClasePPYE { get; set; }
        public string Tipo { get; set; }
        public long IdFacturasProductos { get; set; }
        public string Documento { get; set; }
        public string Sucursal { get; set; }

        public DateTime Fecha { get; set; } 
        public string periodo { get; set; }

    }
}
