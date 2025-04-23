#!/usr/bin/env python3
"""
Script to train a transformer-based LLM using Wikipedia data from the changelog.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

import numpy as np
import torch
import torch.nn.functional as F
from torch.utils.data import Dataset, DataLoader
from transformers import set_seed
from src.training.transformer import CustomTransformer
from src.training.tokenizer import SimpleTokenizer
# Import the appropriate logger based on the file extension
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

def get_appropriate_logger(changelog_path, debug=False):
    """
    Get the appropriate logger based on the file extension.
    
    Args:
        changelog_path: Path to the changelog file
        debug: Enable debug logging
        
    Returns:
        The appropriate logger instance
    """
    path = Path(changelog_path)
    if path.suffix.lower() == '.db':
        logger.info(f"Using ChangelogDB for {changelog_path}")
        from src.db.changelog_db import ChangelogDB
        return ChangelogDB(changelog_path, debug=debug)
    else:
        logger.info(f"Using ChangelogLogger for {changelog_path}")
        from src.changelog.logger import ChangelogLogger
        return ChangelogLogger(changelog_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WikipediaDataset(Dataset):
    """Dataset for Wikipedia pages from changelog."""

    def __init__(
        self,
        raw_data_path: Path,
        page_ids: List[str],
        tokenizer: SimpleTokenizer,
        max_length: int = 512
    ):
        """
        Initialize dataset.

        Args:
            raw_data_path: Path to raw data directory
            page_ids: List of page IDs to include
            tokenizer: Custom tokenizer
            max_length: Maximum sequence length
        """
        self.raw_data_path = raw_data_path
        self.page_ids = page_ids
        self.tokenizer = tokenizer
        self.max_length = max_length
    def __len__(self) -> int:
        return len(self.page_ids)

    def __getitem__(self, idx: int) -> Dict[str, List[int]]:
        """Get tokenized page content."""
        page_id = self.page_ids[idx]
        
        # Ensure page_id is a string, not bytes
        if isinstance(page_id, bytes):
            page_id = page_id.decode('utf-8')
            
        file_path = self.raw_data_path / f"{page_id}.txt"
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except FileNotFoundError:
            # Enhanced error reporting
            is_revision = '_' in str(page_id)
            file_type = "revision" if is_revision else "main page"
            print(f"File not found: {file_path} (file type: {file_type})")
            print(f"Page ID type: {type(page_id)}, value: {page_id}")
            
            # Check if the raw_data_path exists
            if not self.raw_data_path.exists():
                print(f"ERROR: Raw data directory does not exist: {self.raw_data_path}")
            else:
                # List some files in the directory to verify content
                files = list(self.raw_data_path.glob("*.txt"))[:5]
                print(f"First few files in {self.raw_data_path}: {[f.name for f in files]}")
                print(f"Total files in directory: {len(list(self.raw_data_path.glob('*.txt')))}")
            
            # Return a minimal set of input_ids to avoid crashing
            return {"input_ids": [0]}  # Use padding token as fallback

        # Tokenize content with BPE-dropout during training (10% dropout probability)
        tokens = self.tokenizer._tokenize(content, dropout_prob=0.1)[:self.max_length]
        input_ids = [self.tokenizer._convert_token_to_id(t) for t in tokens]

        # Return raw input_ids for data collator to handle padding
        return {"input_ids": input_ids}

class LLMTrainer:
    """Handles LLM training using Wikipedia data."""

    def __init__(
        self,
        model_path: Optional[str] = None,
        changelog_path: str = "data/changelog.db",
        raw_data_path: str = "data/raw",
        output_dir: str = "models",
        max_length: int = 512,
        batch_size: int = 4,
        learning_rate: float = 1e-4,
        num_epochs: int = 3,
        seed: int = 42,
        d_model: int = 256,
        num_heads: int = 4,
        num_layers: int = 4,
        d_ff: int = 512,
        debug: bool = False
    ):
        """
        Initialize trainer.

        Args:
            model_path: Path to existing model to continue training from, or None to start fresh
            changelog_path: Path to changelog file
            raw_data_path: Path to raw data directory
            output_dir: Directory to save model checkpoints
            max_length: Maximum sequence length
            batch_size: Training batch size
            learning_rate: Learning rate
            num_epochs: Number of training epochs
            seed: Random seed
            vocab_size: Size of vocabulary for custom tokenizer
            d_model: Model dimension for custom transformer
            num_heads: Number of attention heads
            num_layers: Number of transformer layers
            d_ff: Feed-forward dimension
            debug: Enable debug logging
        """
        self.model_path = model_path
        self.debug = debug
        self.changelog = get_appropriate_logger(changelog_path, debug=debug)
        self.raw_data_path = Path(raw_data_path)
        self.output_dir = Path(output_dir)
        self.max_length = max_length
        self.batch_size = batch_size
        self.learning_rate = learning_rate
        self.num_epochs = num_epochs
        self.seed = seed
        self.d_model = d_model
        self.num_heads = num_heads
        self.num_layers = num_layers
        self.d_ff = d_ff

        # Set random seed
        set_seed(seed)
        
        # Initialize or load model and tokenizer
        model_dir = Path(model_path) if model_path else None
        
        # Add detailed logging for path validation
        if model_path:
            logger.info(f"Checking for existing model at path: {model_path}")
            logger.info(f"Absolute path: {Path(model_path).absolute()}")
            logger.info(f"Path exists: {Path(model_path).exists()}")
            
            # Check for required files
            if Path(model_path).exists():
                required_files = ["config.json", "pytorch_model.bin", "tokenizer_config.json", "vocab.json"]
                missing_files = [f for f in required_files if not (Path(model_path) / f).exists()]
                
                if missing_files:
                    logger.warning(f"Model directory exists but missing files: {', '.join(missing_files)}")
                else:
                    logger.info(f"All required model files found in {model_path}")
        
        # Try to load tokenizer from model_path first (if provided)
        tokenizer_loaded = False
        if model_dir and model_dir.exists() and (model_dir / "vocab.json").exists():
            logger.info(f"Loading tokenizer from model path: {model_dir}")
            try:
                self.tokenizer = SimpleTokenizer.from_pretrained(str(model_dir))
                logger.info(f"Tokenizer loaded from model path. Vocabulary size: {len(self.tokenizer)}")
                tokenizer_loaded = True
            except Exception as e:
                logger.warning(f"Error loading tokenizer from model path: {str(e)}")

        # Fall back to models/tokenizer if not loaded from model_path
        if not tokenizer_loaded:
            tokenizer_path = Path("models/tokenizer")
            if not tokenizer_path.exists() or not (tokenizer_path / "vocab.json").exists():
                raise ValueError(
                    f"Pre-trained tokenizer not found at {tokenizer_path}. "
                    f"Please run 'python scripts/train_tokenizer.py' first."
                )

            logger.info(f"Loading pre-trained tokenizer from {tokenizer_path}")
            try:
                self.tokenizer = SimpleTokenizer.from_pretrained(str(tokenizer_path))
                logger.info(f"Pre-trained tokenizer loaded. Vocabulary size: {len(self.tokenizer)}")
            except Exception as e:
                raise ValueError(f"Error loading pre-trained tokenizer: {str(e)}")

        # Load or initialize model
        if model_dir and model_dir.exists() and (model_dir / "config.json").exists():
            # Load model
            logger.info(f"Loading existing model from {model_dir}")
            try:
                self.model = CustomTransformer.from_pretrained(str(model_dir))
                logger.info(f"Model loaded successfully")
            except Exception as e:
                logger.error(f"Error loading model: {str(e)}")
                logger.warning("Initializing a new model")
                self.model = CustomTransformer(
                    vocab_size=len(self.tokenizer),
                    d_model=self.d_model,
                    num_heads=self.num_heads,
                    num_layers=self.num_layers,
                    d_ff=self.d_ff,
                    max_seq_length=self.max_length
                )
        else:
            # Initialize fresh model
            if model_path:
                logger.warning(f"Model path {model_path} does not exist or is missing required files")
            logger.info("Initializing new transformer model...")
            self.model = CustomTransformer(
                vocab_size=len(self.tokenizer),
                d_model=self.d_model,
                num_heads=self.num_heads,
                num_layers=self.num_layers,
                d_ff=self.d_ff,
                max_seq_length=self.max_length
            )
    
    # Removed _train_new_tokenizer method as tokenizer training is now a separate process

    def _compute_checkpoint_hash(self) -> str:
        """Compute hash of model state."""
        state_dict = self.model.state_dict()
        hasher = hashlib.sha256()
        
        # Sort keys for consistent ordering
        for key in sorted(state_dict.keys()):
            hasher.update(state_dict[key].cpu().numpy().tobytes())
        
        return hasher.hexdigest()

    def _collate_fn(self, examples: List[Dict[str, List[int]]]) -> Dict[str, torch.Tensor]:
        """Custom collate function for batching."""
        # Get all input_ids
        input_ids = [example["input_ids"] for example in examples]
        
        # Find max length in batch
        max_len = max(len(ids) for ids in input_ids)
        
        # Pad input_ids and create attention masks
        padded_input_ids = []
        attention_mask = []
        for ids in input_ids:
            padding_length = max_len - len(ids)
            padded_input_ids.append(ids + [self.tokenizer._convert_token_to_id(self.tokenizer.pad_token)] * padding_length)
            attention_mask.append([1] * len(ids) + [0] * padding_length)
        
        # Convert to tensors
        batch = {
            "input_ids": torch.tensor(padded_input_ids),
            "attention_mask": torch.tensor(attention_mask)
        }
        
        return batch

    def train(
        self,
        val_split: float = 0.1,
        min_pages: int = 100
    ) -> None:
        """
        Train the model on unused Wikipedia pages.

        Args:
            val_split: Validation split ratio
            min_pages: Minimum number of pages required
        """
        # Get unused pages
        unused_pages = self.changelog.get_unused_pages()
        if len(unused_pages) < min_pages:
            raise ValueError(
                f"Not enough unused pages. Found {len(unused_pages)}, "
                f"need at least {min_pages}"
            )

        # Get page IDs
        page_ids = [entry["page_id"] for entry in unused_pages]
        
        # Filter out page IDs that don't have corresponding files in raw_data_path
        existing_page_ids = []
        for page_id in page_ids:
            # Ensure page_id is a string, not bytes
            if isinstance(page_id, bytes):
                page_id = page_id.decode('utf-8')
                
            file_path = self.raw_data_path / f"{page_id}.txt"
            if file_path.exists():
                existing_page_ids.append(page_id)
            elif self.debug:
                logger.debug(f"Skipping page_id {page_id} - file not found: {file_path}")
        
        logger.info(f"Found {len(existing_page_ids)} pages with existing files out of {len(page_ids)} unused pages")
        
        if len(existing_page_ids) < min_pages:
            raise ValueError(
                f"Not enough unused pages with existing files. Found {len(existing_page_ids)}, "
                f"need at least {min_pages}"
            )
            
        # Use only existing page IDs
        page_ids = existing_page_ids

        # Split into train/val
        np.random.shuffle(page_ids)
        split_idx = int(len(page_ids) * (1 - val_split))
        train_ids = page_ids[:split_idx]
        val_ids = page_ids[split_idx:]

        logger.info(f"Training on {len(train_ids)} pages")
        logger.info(f"Validating on {len(val_ids)} pages")

        # Create datasets
        train_dataset = WikipediaDataset(
            self.raw_data_path,
            train_ids,
            self.tokenizer,
            self.max_length
        )
        val_dataset = WikipediaDataset(
            self.raw_data_path,
            val_ids,
            self.tokenizer,
            self.max_length
        )

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Create data loaders
        train_loader = DataLoader(
            train_dataset,
            batch_size=self.batch_size,
            shuffle=True,
            collate_fn=self._collate_fn
        )
        val_loader = DataLoader(
            val_dataset,
            batch_size=self.batch_size,
            shuffle=False,
            collate_fn=self._collate_fn
        )

        # Setup optimizer
        optimizer = torch.optim.AdamW(
            self.model.parameters(),
            lr=self.learning_rate,
            betas=(0.9, 0.999),
            eps=1e-8,
            weight_decay=0.01
        )

        # Training loop
        self.model.train()
        for epoch in range(self.num_epochs):
            total_loss = 0
            for batch_idx, batch in enumerate(train_loader):
                input_ids = batch["input_ids"]
                attention_mask = batch["attention_mask"]
                labels = input_ids.clone()

                # Initialize metrics dictionary at the start of training
                if epoch == 0 and batch_idx == 0:
                    training_metrics = {}
                    # Calculate initial loss for all pages
                    with torch.no_grad():
                        initial_logits = self.model(x=input_ids, attention_mask=attention_mask)
                        initial_loss = F.cross_entropy(
                            initial_logits.view(-1, initial_logits.size(-1)),
                            labels.view(-1),
                            ignore_index=self.tokenizer._convert_token_to_id(self.tokenizer.pad_token),
                            reduction='none'
                        )
                        initial_token_loss = initial_loss.view(input_ids.shape)
                        initial_sequence_loss = initial_token_loss.sum(dim=1) / (attention_mask.sum(dim=1).float() + 1e-8)
                        initial_loss_value = initial_sequence_loss[0].item()
                        
                        for page_id in train_ids:
                            training_metrics[page_id] = {
                                "initial_loss": initial_loss_value,
                                "average_loss": [],
                                "token_impact": []
                            }
                
                if torch.isnan(input_ids).any():
                    print(f"⚠️ NaN detected in input_ids at Epoch {epoch}, Batch {batch_idx}!")
                
                if torch.isnan(attention_mask).any():
                    print(f"⚠️ NaN detected in attention_mask at Epoch {epoch}, Batch {batch_idx}!")
                
                # Forward pass with metrics collection
                logits = self.model(x=input_ids, attention_mask=attention_mask, store_metrics=True)

                # debug: chekc if logits contain NaN
                if torch.isnan(logits).any():
                    print(f"⚠️ NaN detected in logits at Epoch {epoch}, Batch {batch_idx}!")
                    print(f"Max logit value: {torch.max(logits).item()}")
                    print(f"Min logit value: {torch.min(logits).item()}")

                # Normalize lgoits to prevent NaN issues with minimal impact
                logits = logits - logits.max(dim=-1,keepdim=True).values
                logits = torch.clamp(logits, min=-10, max=10)  # Clip extreme values
                
                # Debug: Check for NaN in logits before computing loss
                if torch.isnan(logits).any():
                    print(f"⚠️ NaN detected in logits BEFORE loss at Epoch {epoch}, Batch {batch_idx}!")
                    print(f"Max logit value: {torch.max(logits).item()}")
                    print(f"Min logit value: {torch.min(logits).item()}")
                
                # Calculate loss
                loss = F.cross_entropy(
                    logits.view(-1, logits.size(-1)),
                    labels.view(-1),
                    ignore_index=self.tokenizer._convert_token_to_id(self.tokenizer.pad_token),
                    reduction='none'
                )

                # debug: check if loss contains NaN
                if torch.isnan(loss).any():
                    print(f"⚠️ NaN detected in loss at Epoch {epoch}, Batch {batch_idx}!")
                
                # Reshape loss to match input shape for per-token metrics
                token_loss = loss.view(input_ids.shape)
                
                # Calculate relative loss for each sequence in batch
                #sequence_loss = token_loss.sum(dim=1) / (attention_mask.sum(dim=1).float() + 1e-8)
                sequence_loss = token_loss.sum(dim=1) / torch.clamp(attention_mask.sum(dim=1).float(),min=1)
                batch_loss = sequence_loss.mean()
                
                # Store metrics for each sequence
                start_idx = batch_idx * self.batch_size
                end_idx = min((batch_idx + 1) * self.batch_size, len(train_ids))
                batch_page_ids = train_ids[start_idx:end_idx]
                
                # Update metrics for current batch
                for idx, page_id in enumerate(batch_page_ids):
                    if page_id not in training_metrics:
                        # Calculate initial loss for relative loss metric
                        with torch.no_grad():
                            initial_logits = self.model(x=input_ids, attention_mask=attention_mask)
                            initial_loss = F.cross_entropy(
                                initial_logits.view(-1, initial_logits.size(-1)),
                                labels.view(-1),
                                ignore_index=self.tokenizer._convert_token_to_id(self.tokenizer.pad_token),
                                reduction='none'
                            )
                            initial_token_loss = initial_loss.view(input_ids.shape)
                            initial_sequence_loss = initial_token_loss.sum(dim=1) / (attention_mask.sum(dim=1).float() + 1e-8)

                        training_metrics[page_id] = {
                            "initial_loss": initial_sequence_loss[idx].item(),
                            "average_loss": [],
                            "token_impact": []
                        }
                    
                    # Store average loss
                    training_metrics[page_id]["average_loss"].append(sequence_loss[idx].item())
                    
                    # Store token impact if available
                    token_impacts = self.model.get_token_impacts()
                    if token_impacts is not None:
                        # Get token IDs and impact values for this sequence
                        sequence_tokens = input_ids[idx].cpu().numpy()
                        impact_values = token_impacts[idx].detach().cpu().numpy().flatten()
                        
                        # Store token impacts with context
                        sequence_tokens = sequence_tokens.tolist()
                        impact_values = impact_values.tolist()
                        
                        # Calculate significance threshold (95th percentile)
                        abs_impacts = [abs(x) for x in impact_values]
                        threshold = sorted(abs_impacts)[int(len(abs_impacts) * 0.95)]
                        
                        # Find critical tokens with context
                        critical_tokens = []
                        context_window = 2  # tokens before and after
                        
                        for i, (token_id, impact) in enumerate(zip(sequence_tokens, impact_values)):
                            if abs(impact) >= threshold:
                                start_pos = max(0, i - context_window)
                                end_pos = min(len(sequence_tokens), i + context_window + 1)
                                critical_tokens.append({
                                    "token_id": token_id,
                                    "position": i,
                                    "impact": float(impact),
                                    "context": [start_pos, end_pos]
                                })
                        
                        training_metrics[page_id]["token_impact"].append({
                            "critical_tokens": critical_tokens,
                            "impact_threshold": float(threshold),
                            "total_tokens": len(sequence_tokens)
                        })

                # Backward pass
                batch_loss.backward()

                # debug: check if gradients contain NaN
                for name, param in self.model.named_parameters():
                    if param.grad is not None and torch.isnan(param.grad).any():
                        print(f"⚠️ NaN detected in gradients of {name} at Epoch {epoch}, Batch {batch_idx}!")

                # Adaptive gradient clipping (Prevents extreme updates while allowing learning)
                torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=max(0.5,0.1 * batch_loss.item()))
                
                optimizer.step()
                optimizer.zero_grad()

                total_loss += batch_loss.item()

                if batch_idx % 10 == 0:
                    logger.info(
                        f"Epoch {epoch+1}/{self.num_epochs} | "
                        f"Batch {batch_idx}/{len(train_loader)} | "
                        f"Loss: {batch_loss.item():.4f}"
                    )

            avg_loss = total_loss / len(train_loader)
            logger.info(f"Epoch {epoch+1} average loss: {avg_loss:.4f}")

            # Save intermediate metrics after each epoch
            checkpoint_hash = self._compute_checkpoint_hash()
            intermediate_metrics = {}
            for page_id, metrics in training_metrics.items():
                # Calculate average loss and relative loss
                avg_loss = sum(metrics["average_loss"]) / len(metrics["average_loss"])
                rel_loss = (metrics["initial_loss"] - avg_loss) / metrics["initial_loss"]
                
                # For token impact, use the last epoch's values and format correctly for db_utils.mark_used_in_training
                token_impact = None
                if metrics["token_impact"] and len(metrics["token_impact"]) > 0:
                    # Get the last epoch's token impact data
                    last_impact = metrics["token_impact"][-1]
                    
                    # Format token impact data correctly for machine unlearning experiments
                    # The db_utils.mark_used_in_training function expects "top_tokens" instead of "critical_tokens"
                    token_impact = {
                        "top_tokens": last_impact["critical_tokens"],  # Rename critical_tokens to top_tokens
                        "total_tokens": last_impact["total_tokens"]
                    }
                    
                    # Always log the token impact data for debugging
                    logger.info(f"Token impact data for {page_id}: {token_impact}")
                    print(f"Token impact data for {page_id}: top_tokens={len(token_impact['top_tokens'])}, total_tokens={token_impact['total_tokens']}")
                
                # Only include metrics if we have valid values
                metrics_dict = {
                    "average_loss": float(avg_loss),
                    "relative_loss": float(rel_loss)
                }
                
                if token_impact is not None:
                    metrics_dict["token_impact"] = token_impact
                
                intermediate_metrics[page_id] = metrics_dict

            logger.info(f"Saving intermediate metrics for epoch {epoch+1}...")
            try:
                # Only mark training pages as used, since we only have metrics for them
                self.changelog.mark_used_in_training(train_ids, checkpoint_hash, intermediate_metrics)
                logger.info(f"Successfully saved metrics for epoch {epoch+1}")
            except Exception as e:
                logger.error(f"Failed to save intermediate metrics: {str(e)}")

        # Save final model
        final_output_dir = self.output_dir / "final"
        self.model.save_pretrained(final_output_dir)
        self.tokenizer.save_pretrained(final_output_dir)

        # Final metrics are already saved in the last epoch
        logger.info(f"Training complete. Model saved to {final_output_dir}")
        logger.info(f"Final checkpoint hash: {checkpoint_hash}")
        
        # Test model with a simple prompt
        test_output = self.generate_text("The quick brown fox", max_length=50)
        logger.info(f"Test generation:\n{test_output}")

        # Remove unused entries from changelog
        logger.info("Removing unused entries from changelog...")
        self.changelog.remove_unused_entries()
        logger.info("Unused entries removed")

    def generate_text(
        self,
        prompt: str,
        max_length: int = 100,
        temperature: float = 0.7,
        top_k: int = 50,
        top_p: float = 0.9
    ) -> str:
        """Generate text from a prompt."""
        self.model.eval()
        
        # Tokenize prompt (no dropout during inference)
        input_ids = self.tokenizer.encode(
            prompt,
            truncation=True,
            max_length=self.max_length,
            return_tensors="pt",
            dropout_prob=0.0  # Ensure no dropout during inference
        )
        
        # Generate text using custom model
        output_ids = self.model.generate(
            input_ids,
            max_length=max_length,
            temperature=temperature,
            top_k=top_k,
            top_p=top_p
        )
        
        # Decode output tokens
        return self.tokenizer.decode(
            output_ids[0],
            skip_special_tokens=True,
            clean_up_tokenization_spaces=True
        )

def load_model(model_path: str, debug: bool = False) -> LLMTrainer:
    """Load a trained model."""
    if not Path(model_path).exists():
        raise ValueError(f"Model path {model_path} does not exist")
        
    # Create trainer instance with model path
    trainer = LLMTrainer(model_path=model_path, debug=debug)
    return trainer

def main():
    parser = argparse.ArgumentParser(
        description="Train LLM on Wikipedia data from changelog"
    )
    parser.add_argument(
        "--load-model",
        help="Load and test a trained model from the specified path"
    )
    parser.add_argument(
        "--test-prompt",
        default="The quick brown fox",
        help="Prompt to test the model with"
    )
    parser.add_argument(
        "--model-path",
        help="Path to existing model to continue training from, or none to start fresh"
    )
    # Removed vocab-size parameter as it's now determined by the pre-trained tokenizer
    parser.add_argument(
        "--d-model",
        type=int,
        default=256,
        help="Model dimension for custom transformer"
    )
    parser.add_argument(
        "--num-heads",
        type=int,
        default=4,
        help="Number of attention heads"
    )
    parser.add_argument(
        "--num-layers",
        type=int,
        default=4,
        help="Number of transformer layers"
    )
    parser.add_argument(
        "--d-ff",
        type=int,
        default=512,
        help="Feed-forward dimension"
    )
    parser.add_argument(
        "--max-length",
        type=int,
        default=1024,
        help="Maximum sequence length"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=4,
        help="Training batch size"
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=2e-5,
        help="Learning rate"
    )
    parser.add_argument(
        "--num-epochs",
        type=int,
        default=3,
        help="Number of training epochs"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed"
    )
    parser.add_argument(
        "--val-split",
        type=float,
        default=0.1,
        help="Validation split ratio"
    )
    parser.add_argument(
        "--min-pages",
        type=int,
        default=100,
        help="Minimum number of pages required"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    # Removed retrain-tokenizer parameter as tokenizer training is now a separate process
    args = parser.parse_args()

    if args.load_model:
        # Load and test model
        trainer = load_model(args.load_model, debug=args.debug)
        output = trainer.generate_text(args.test_prompt, max_length=100)
        print(f"\nInput prompt: {args.test_prompt}")
        print(f"Generated text:\n{output}\n")
    else:
        # Train model (either fresh or continue training)
        trainer = LLMTrainer(
            model_path=args.model_path,  # None for fresh, path for continue
            max_length=args.max_length,
            batch_size=args.batch_size,
            learning_rate=args.learning_rate,
            num_epochs=args.num_epochs,
            seed=args.seed,
            d_model=args.d_model,
            num_heads=args.num_heads,
            num_layers=args.num_layers,
            d_ff=args.d_ff,
            debug=args.debug
        )

        trainer.train(
            val_split=args.val_split,
            min_pages=args.min_pages
        )

if __name__ == "__main__":
    main()
