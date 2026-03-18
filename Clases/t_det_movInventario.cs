using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Migracion.Clases
{
    internal class t_det_movInventario
    {
        public long? IdMovimiento { get; set; }
        public string IdProducto { get; set; }
        public string IdBodega { get; set; }
        public long? Cantidad { get; set; }
        public long? Recibida { get; set; }
        public string TipoMov { get; set; }
        public long? CostoUnitarioBruto { get; set; }
        public int PorcentajeDescuento { get; set; }
        public int Descuento { get; set; }
        public long? CostoUnitarioNeto { get; set; }
        public long? CostoTotal { get; set; }
        public long? PorcentajeIVA { get; set; }
        public long? ValorIVA { get; set; }
        public long? CostoTotalIVA { get; set; }
        public long? ImpuestoConsumo { get; set; }
        public int PorcentajeRentabilidad { get; set; }
        public DateTime FechaCreacion { get; set; }
        public int Fletes { get; set; }
        public string Observaciones { get; set; }


        // campos para busqueda 
        public int numero { get; set; }
        public string documento { get; set; }

    }
}
