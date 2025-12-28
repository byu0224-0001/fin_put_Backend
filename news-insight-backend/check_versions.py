"""라이브러리 버전 확인"""
import sys

print("=" * 50)
print("라이브러리 버전 확인")
print("=" * 50)

try:
    import torch
    print(f"PyTorch: {torch.__version__}")
    print(f"  - CUDA available: {torch.cuda.is_available()}")
    if torch.cuda.is_available():
        print(f"  - CUDA version: {torch.version.cuda}")
        print(f"  - GPU: {torch.cuda.get_device_name(0)}")
except Exception as e:
    print(f"PyTorch: ERROR - {e}")

try:
    import transformers
    print(f"Transformers: {transformers.__version__}")
except Exception as e:
    print(f"Transformers: ERROR - {e}")

try:
    import sentence_transformers
    print(f"SentenceTransformers: {sentence_transformers.__version__}")
except Exception as e:
    print(f"SentenceTransformers: ERROR - {e}")

try:
    import accelerate
    print(f"Accelerate: {accelerate.__version__}")
except ImportError:
    print("Accelerate: NOT INSTALLED ⚠️")
except Exception as e:
    print(f"Accelerate: ERROR - {e}")

try:
    from FlagEmbedding import BGEM3FlagModel
    print("FlagEmbedding: INSTALLED ✅")
except ImportError:
    print("FlagEmbedding: NOT INSTALLED ⚠️")
except Exception as e:
    print(f"FlagEmbedding: ERROR - {e}")

print("\n" + "=" * 50)
print("Meta Tensor 테스트")
print("=" * 50)

# 간단한 모델 로드 테스트
try:
    from transformers import AutoModel, AutoTokenizer
    import os
    
    # 환경 변수 설정
    os.environ['TRANSFORMERS_OFFLINE'] = '0'
    
    print("AutoModel.from_pretrained() 테스트...")
    model_name = "upskyy/kf-deberta-multitask"
    
    # 다양한 옵션으로 테스트
    print(f"\n1. 기본 로드 테스트 (옵션 없음)...")
    try:
        model = AutoModel.from_pretrained(model_name)
        print("   ✅ 성공!")
        # 현재 device 확인
        print(f"   - Model device: {next(model.parameters()).device}")
        del model
    except Exception as e:
        print(f"   ❌ 실패: {e}")
    
    print(f"\n2. low_cpu_mem_usage=False 테스트...")
    try:
        model = AutoModel.from_pretrained(model_name, low_cpu_mem_usage=False)
        print("   ✅ 성공!")
        print(f"   - Model device: {next(model.parameters()).device}")
        
        # to('cpu') 테스트
        print("\n3. to('cpu') 테스트...")
        try:
            model = model.to('cpu')
            print("   ✅ 성공!")
        except Exception as e:
            print(f"   ❌ 실패: {e}")
        
        # to('cuda') 테스트
        if torch.cuda.is_available():
            print("\n4. to('cuda') 테스트...")
            try:
                model = model.to('cuda')
                print("   ✅ 성공!")
                print(f"   - Model device: {next(model.parameters()).device}")
            except Exception as e:
                print(f"   ❌ 실패: {e}")
        
        del model
    except Exception as e:
        print(f"   ❌ 실패: {e}")
    
except Exception as e:
    print(f"테스트 실패: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 50)
print("SentenceTransformer 테스트")
print("=" * 50)

try:
    from sentence_transformers import SentenceTransformer
    
    print("\n1. SentenceTransformer 기본 로드 테스트...")
    try:
        model = SentenceTransformer("upskyy/kf-deberta-multitask")
        print("   ✅ 성공!")
        print(f"   - Device: {model.device}")
        del model
    except Exception as e:
        print(f"   ❌ 실패: {e}")
    
    print("\n2. SentenceTransformer device='cpu' 테스트...")
    try:
        model = SentenceTransformer("upskyy/kf-deberta-multitask", device='cpu')
        print("   ✅ 성공!")
        print(f"   - Device: {model.device}")
        del model
    except Exception as e:
        print(f"   ❌ 실패: {e}")
    
    if torch.cuda.is_available():
        print("\n3. SentenceTransformer device='cuda' 테스트...")
        try:
            model = SentenceTransformer("upskyy/kf-deberta-multitask", device='cuda')
            print("   ✅ 성공!")
            print(f"   - Device: {model.device}")
            del model
        except Exception as e:
            print(f"   ❌ 실패: {e}")

except Exception as e:
    print(f"SentenceTransformer 테스트 실패: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 50)
print("테스트 완료")
print("=" * 50)



