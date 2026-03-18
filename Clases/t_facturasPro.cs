using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    public class t_facturasPro
    {
        public int Numero { get; set; }
        public DateTime Fecha { get; set; }
        public long? IdProveedor { get; set; }
        public string IdDocumento { get; set; }
        public int IdEstado { get; set; }
        public string IdSucursal { get; set; }
        public string IdOrdenCompra { get; set; }
        public int Subtotal { get; set; }
        public int Total { get; set; }
        public string CxPagar { get; set; }
        public int DiasCredito { get; set; }
        public DateTime FechaRecibido { get; set; }
        public string Observaciones { get; set; }
        public int IVAFletes { get; set; }
        public string IdBodega { get; set; }
        public string IdCentroCosto { get; set; }
        public int Contado { get; set; }
        public string Receptor { get; set; }
        public int Entradas { get; set; }
        public string CGrabado { get; set; }
        public string CExcluido { get; set; }
        public int IConsumo { get; set; }
        public int Flete { get; set; }
        public int ValorIVA { get; set; }
        public int ValorRetencion { get; set; }
        public int Fenaice { get; set; }
        public int ReteICA { get; set; }
        public string IdRFte { get; set; }
        public int RFte { get; set; }
        public string IdRCree { get; set; }
        public int RCree { get; set; }
        public int Asohofrucol { get; set; }
        public int FNFP { get; set; }
        public string idformapago { get; set; }
        public string Periodo { get; set; }
        public int Consecutivo { get; set; }
        public string IdUsuarioElaboro { get; set; }
        public string EstadoRecepcion { get; set; }

        public string CxP { get; set; }
    }
}
