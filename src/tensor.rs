use std::{collections::HashMap, error::Error};

use a5_geozarr::query::CoordValues;
use datafusion::arrow::array::StringBuilder;
use ndarray::{ArrayBase, Data, Dimension};
use rdf_fusion::encoding::typed_family::UnknownFamilyArray;
use serde::Serialize;

pub const DATA_TENSOR_DATATYPE: &str = "https://w3id.org/rdf-tensor/vocab#DataTensor";

#[derive(Serialize)]
#[serde(untagged)]
pub enum TensorValue {
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
}

#[derive(Serialize)]
struct DataTensorJson<'a> {
    #[serde(rename = "type")]
    tensor_type: &'a str,
    shape: Vec<usize>,
    data: Vec<TensorValue>,
    coords: HashMap<String, CoordValues>,
}

pub trait IntoTensorValue {
    const TENSOR_TYPE: &'static str;
    fn into_tensor_value(self) -> TensorValue;
}

impl IntoTensorValue for bool {
    const TENSOR_TYPE: &'static str = "bool";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::Bool(self)
    }
}
impl IntoTensorValue for u8 {
    const TENSOR_TYPE: &'static str = "uint8";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::U8(self)
    }
}
impl IntoTensorValue for u16 {
    const TENSOR_TYPE: &'static str = "uint16";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::U16(self)
    }
}
impl IntoTensorValue for u32 {
    const TENSOR_TYPE: &'static str = "uint32";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::U32(self)
    }
}
impl IntoTensorValue for u64 {
    const TENSOR_TYPE: &'static str = "uint64";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::U64(self)
    }
}
impl IntoTensorValue for i16 {
    const TENSOR_TYPE: &'static str = "int16";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::I16(self)
    }
}
impl IntoTensorValue for i32 {
    const TENSOR_TYPE: &'static str = "int32";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::I32(self)
    }
}
impl IntoTensorValue for i64 {
    const TENSOR_TYPE: &'static str = "int64";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::I64(self)
    }
}
impl IntoTensorValue for f32 {
    const TENSOR_TYPE: &'static str = "float32";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::F32(self)
    }
}
impl IntoTensorValue for f64 {
    const TENSOR_TYPE: &'static str = "float64";
    fn into_tensor_value(self) -> TensorValue {
        TensorValue::F64(self)
    }
}

#[derive(Debug)]
pub struct RdfLiteral {
    pub value: String,
    pub datatype: &'static str,
}

impl RdfLiteral {
    pub fn from_sample<S, D, T>(
        sample: Option<ArrayBase<S, D>>,
        coords: Option<HashMap<String, CoordValues>>,
    ) -> Result<Self, Box<dyn Error + Send + Sync + 'static>>
    where
        S: Data<Elem = T>,
        D: Dimension,
        T: IntoTensorValue + Clone,
    {
        let Some(arr) = sample else {
            // UnknownFamilyArray does not allow null values
            return Ok(RdfLiteral {
                value: "".to_string(),
                datatype: DATA_TENSOR_DATATYPE,
            });
        };

        let Some(coords) = coords else {
            return Err("Sample does not have coordinate values".into());
        };

        let shape = arr.shape().to_vec();
        let contiguous = arr.as_standard_layout();
        let data: Vec<TensorValue> = contiguous.into_iter().map(T::into_tensor_value).collect();

        let payload = DataTensorJson {
            tensor_type: T::TENSOR_TYPE,
            shape,
            data,
            coords,
        };

        let json = serde_json::to_string(&payload)?;
        Ok(RdfLiteral {
            value: json,
            datatype: DATA_TENSOR_DATATYPE,
        })
    }

    pub fn into_unknown_family_array(
        literals: Vec<Self>,
    ) -> Result<UnknownFamilyArray, datafusion::arrow::error::ArrowError> {
        let mut values_builder = StringBuilder::with_capacity(literals.len(), literals.len() * 64);
        let mut datatypes_builder =
            StringBuilder::with_capacity(literals.len(), literals.len() * 48);

        for lit in literals {
            values_builder.append_value(lit.value);
            datatypes_builder.append_value(lit.datatype);
        }

        let values = values_builder.finish();
        let datatypes = datatypes_builder.finish();

        UnknownFamilyArray::try_new(values, datatypes)
    }
}
