#include "typedbytes.h"
#include "stdio.h"

#include <string>
#include <vector>

#define IS_TYPEDBYTES_BYTE_SEQUENCE(type) ((type) >= 50 && (type) <= 200)

static inline void push_opaque_typecode(typedbytes_opaque& buffer,
                                        TypedBytesType code) {
  buffer.push_back((unsigned char) code);
}

static inline void push_opaque_bytes(typedbytes_opaque& buffer,
                                     unsigned char* bytes, size_t size) {
  while (size > 0) {
    buffer.push_back(*bytes);
    ++bytes;
    --size;
  }
}
    
static inline void push_opaque_length(typedbytes_opaque& buffer,
                                      typedbytes_length len) {
  len = bswap32(len);
  push_opaque_bytes(buffer, (unsigned char*) &len, sizeof(typedbytes_length));
}

bool TypedBytesInFile::_read_opaque_primitive(typedbytes_opaque& buffer, 
                                              TypedBytesType type) {
  // TODO check the fread commands in this function
  unsigned char bytebuf = 0;
  int32_t intbuf = 0;
  int64_t longbuf = 0;
  typedbytes_length len = 0;
    
  // NOTE the typecode has already been pushed
    
  // translate this type to avoid nastiness in the switch.
  if (IS_TYPEDBYTES_BYTE_SEQUENCE(type))
    type = TypedBytesByteSequence;

  switch (type) {
  case TypedBytesByte:
  case TypedBytesBoolean:
    fread(&bytebuf, sizeof(unsigned char), 1, stream_);
    push_opaque_bytes(buffer, &bytebuf, sizeof(unsigned char));
    break;
            
  case TypedBytesInteger:
  case TypedBytesFloat:
    fread(&intbuf, sizeof(int32_t), 1, stream_);
    push_opaque_bytes(buffer, (unsigned char*) &intbuf, sizeof(int32_t));
    break;
            
  case TypedBytesLong:
  case TypedBytesDouble:
    fread(&longbuf, sizeof(int64_t), 1, stream_);
    push_opaque_bytes(buffer, (unsigned char*) &longbuf, sizeof(int64_t));
    break;
            
  case TypedBytesString:
  case TypedBytesByteSequence:
    len = _read_length();
    while (len > 0) {
      // stream_ to buffer in longbuf bytes at a time.
      if (len >= 8) {
        fread(&longbuf, sizeof(int64_t), 1, stream_);
        push_opaque_bytes(buffer, (unsigned char*) &longbuf, sizeof(int64_t));
        len -= sizeof(int64_t);
      } else {
        fread(&longbuf, len, 1, stream_);
        push_opaque_bytes(buffer, (unsigned char*) &longbuf, len);
        break;
      }
    }
    break;

  default:
    return false;
  }
  return true;
}

bool TypedBytesInFile::_read_opaque(typedbytes_opaque& buffer, bool list) {
  TypedBytesType type = next_type();
  push_opaque_typecode(buffer, type);
  typedbytes_length len;
  switch (type) {
  case TypedBytesByteSequence:
  case TypedBytesByte:
  case TypedBytesBoolean:
  case TypedBytesInteger:
  case TypedBytesLong:
  case TypedBytesFloat:
  case TypedBytesDouble:
  case TypedBytesString:
    _read_opaque_primitive(buffer, type);
    break;
  case TypedBytesVector:
    len = read_typedbytes_sequence_length();
    push_opaque_length(buffer, len);
    for (int i = 0; i < len; ++i) {
      _read_opaque(buffer, false);
    }
    break;
  case TypedBytesMap:
    len = read_typedbytes_sequence_length();
    push_opaque_length(buffer, len);
    for (int i = 0; i < len; ++i) {
      _read_opaque(buffer, false);
      _read_opaque(buffer, false);
    }
    break;
  case TypedBytesList:
    while (last_code_ != TypedBytesListEnd) {
      _read_opaque(buffer, true);
    }
    break;
  default:
    return list && type == TypedBytesListEnd;
  }
  return true;
}

bool TypedBytesInFile::read_opaque(typedbytes_opaque& buffer) {
  return _read_opaque(buffer, false);
}


bool TypedBytesInFile::skip_next() {
  // TODO, rewrite these functions to avoid loading into memory
  typedbytes_opaque value;
  return read_opaque(value);
}

TypedBytesType TypedBytesInFile::next_type() {
  unsigned char code = next_type_code();
  if (code <= 10 || code == 255) {
    return (TypedBytesType) code;
  } else if (IS_TYPEDBYTES_BYTE_SEQUENCE(code)) {
    return TypedBytesByteSequence;
  } else if (code == TypedBytesTypeError) {
    // error flag already set in this case.
    return TypedBytesTypeError;
  } else {
    // TODO set error flag
    return TypedBytesTypeError;
  }
}
    
unsigned char TypedBytesInFile::next_type_code() {
  int ch = fgetc(stream_);
  // reset last_length_
  last_length_ = -1;
  if (ch == EOF) {
    // TODO set error flag
    last_code_ = TypedBytesTypeError;
    return (unsigned char) TypedBytesTypeError;
  } else {
    last_code_ = (TypedBytesType) ch;
    return (unsigned char) ch;
  }
}
    
size_t TypedBytesInFile::_read_bytes(void *ptr, size_t nbytes, size_t nelem) {
  size_t nread = fread(ptr, nbytes, nelem, stream_);
  // TODO set error flag and determine more intelligent action.
  assert(nread == nelem);
  // reset last_length_
  last_length_ = -1;
  return nread;
}
    
int32_t TypedBytesInFile::_read_length() {
  int32_t len = 0;
  _read_bytes(&len, sizeof(int32_t), 1);
  len = bswap32(len);
  return len;
}
    
bool TypedBytesInFile::_read_data_block(unsigned char* data, size_t size) {
  assert(last_length_ >= 0);
  typedbytes_length curlen = last_length_;
  if (size > (size_t) curlen) {
    return false;
  }
  size_t nread = _read_bytes(data, sizeof(unsigned char), (size_t) size);
  // NOTE _read_bytes resets last_length_, so we have to reset it back
  if (nread != size) {
    // TODO update error
    return false;
  }
  last_length_ = curlen - size;
  assert(last_length_ >= 0);
  return true;
}
    
#ifdef TYPEDBYTES_STRICT_TYPE
# define typedbytes_check_type_code(x) (assert((x) == last_code_))
#else
# define typedbytes_check_type_code(x)
#endif    

signed char TypedBytesInFile::read_byte() {
  typedbytes_check_type_code(TypedBytesByte);
  signed char rval = 0;
  _read_bytes(&rval, sizeof(signed char), 1);
  return (rval);
}
    
bool TypedBytesInFile::read_bool() {
  typedbytes_check_type_code(TypedBytesBoolean);
  signed char rval = 0;
  _read_bytes(&rval, sizeof(signed char), 1);
  return (bool) rval;
}

float TypedBytesInFile::read_float() {
  typedbytes_check_type_code(TypedBytesFloat);
  int32_t val = 0;
  _read_bytes(&val, sizeof(int32_t), 1);
  val = bswap32(val);
  float rval;
  memcpy(&rval, &val, sizeof(int32_t));
  return rval;
}
    
double TypedBytesInFile::read_double() {
  typedbytes_check_type_code(TypedBytesDouble);
  int64_t val = 0;
  _read_bytes(&val, sizeof(int64_t), 1);
  val = bswap64(val);
  double rval;
  memcpy(&rval, &val, sizeof(int64_t));
  return rval;
}
    
double TypedBytesInFile::convert_double() {
  switch (last_code_) {
  case TypedBytesFloat:
    return (double) read_float();
  case TypedBytesDouble:
    return read_double();
  default:
    return (double) convert_long();
  }
}
    
typedbytes_long TypedBytesInFile::convert_long() {
  if (last_code_ == TypedBytesLong)
    return read_long();
  return (long) convert_int();
}
    
int TypedBytesInFile::convert_int() {
  switch (last_code_) {
  case TypedBytesByte:
    return (int) read_byte();
  case TypedBytesBoolean:
    return (int) read_bool();
  case TypedBytesInteger:
    return read_int();
  default:
    assert(last_code_ == TypedBytesTypeError);
    return 0;
  }
}
    
int TypedBytesInFile::read_int() {
  typedbytes_check_type_code(TypedBytesInteger);
  int32_t rval = 0;
  _read_bytes(&rval, sizeof(int32_t), 1);
  rval = bswap32(rval);
  return (int) rval;
}
    
typedbytes_long TypedBytesInFile::read_long() {
  typedbytes_check_type_code(TypedBytesLong);
  int64_t rval = 0;
  _read_bytes(&rval, sizeof(int64_t), 1);
  rval = bswap64(rval);
  return (typedbytes_long) rval;
}
    

typedbytes_length TypedBytesInFile::read_string_length() {
  typedbytes_check_type_code(TypedBytesString);
  typedbytes_length len = _read_length();
  last_length_ = len;
  return len;
}
    
bool TypedBytesInFile::read_string_data(unsigned char* data, size_t size) {
  typedbytes_check_type_code(TypedBytesString);
  return _read_data_block(data, size);
}
    
bool TypedBytesInFile::read_string(std::string& str) {
  typedbytes_check_type_code(TypedBytesString);
  typedbytes_length len = _read_length();
  str.resize(len);
  assert(len >= 0);
  // TODO check for error
  _read_bytes(&str[0], sizeof(unsigned char), (size_t) len);
  return true;
}
    
typedbytes_length TypedBytesInFile::read_byte_sequence_length() {
#ifdef TYPEDBYTES_STRICT_TYPE        
  if (last_code_ == TypedBytesByteSequence || 
      IS_TYPEDBYTES_BYTE_SEQUENCE(last_code_)) {
    // do nothing here
  } else {
    typedbytes_check_type_code(TypedBytesTypeError);
  }
#endif
  last_length_ = _read_length();
  return last_length_;
}
    
bool TypedBytesInFile::read_byte_sequence(unsigned char* data, size_t size) {
#ifdef TYPEDBYTES_STRICT_TYPE        
  if (last_code_ == TypedBytesByteSequence ||
      IS_TYPEDBYTES_BYTE_SEQUENCE(last_code_)) {
    // do nothing here
  } else {
    typedbytes_check_type_code(TypedBytesTypeError);
  }
#endif
  return _read_data_block(data, size);
}

typedbytes_length TypedBytesInFile::read_typedbytes_sequence_length() {
#ifdef TYPEDBYTES_STRICT_TYPE        
  if (last_code_ == TypedBytesVector || last_code_ == TypedBytesMap) {
    // do nothing here
  } else {
    typedbytes_check_type_code(TypedBytesTypeError);
  }
#endif        
  return _read_length();
}

bool TypedBytesOutFile::_write_length(typedbytes_length len) {
  len = bswap32(len);
  return fwrite(&len, sizeof(typedbytes_length), 1, stream_) == 1;
}

bool TypedBytesOutFile::_write_code(TypedBytesType t) {
  unsigned char code = (unsigned char) t;
  return _write_bytes(&code, 1, 1);
}

bool TypedBytesOutFile::write_bool(bool val) {
  signed char sval = 0;
  if (val) {
    sval = 1;
  }
  return _write_code(TypedBytesBoolean) && _write_bytes(&sval, 1, 1);
}
    
bool TypedBytesOutFile::write_int(int val) {
  int32_t sval = bswap32(val);
  return _write_code(TypedBytesInteger) &&
    _write_bytes(&sval, sizeof(int32_t), 1);
}
    
bool TypedBytesOutFile::write_long(typedbytes_long val) {
  val = bswap64(val);
  return _write_code(TypedBytesLong) &&
    _write_bytes(&val, sizeof(typedbytes_long), 1);
}
    
bool TypedBytesOutFile::write_float(float val) {
  int32_t sval = 0;
  memcpy(&sval, &val, sizeof(int32_t));
  sval = bswap32(sval);
  return _write_code(TypedBytesFloat) && _write_bytes(&sval, sizeof(int32_t), 1);
}
    
bool TypedBytesOutFile::write_double(double val) {
  int64_t sval = 0;
  memcpy(&sval, &val, sizeof(int64_t));
  sval = bswap64(sval);
  return _write_code(TypedBytesDouble) &&
    _write_bytes(&sval, sizeof(int64_t), 1);
}
